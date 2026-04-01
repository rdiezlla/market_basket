from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .config import AppConfig
from .utils import safe_div


@dataclass
class TransactionBuildResult:
    tx_item_df: pd.DataFrame
    transactions_df: pd.DataFrame
    transaction_item_sets: pd.Series
    transaction_quantity_maps: pd.Series


def _resolve_transaction_date(series: pd.Series, strategy: str) -> pd.Timestamp | pd.NaT:
    clean = series.dropna().sort_values()
    if clean.empty:
        return pd.NaT
    if strategy == "max_completion_date":
        return clean.max()
    if strategy == "min_completion_date":
        return clean.min()
    if strategy == "mode_date":
        normalized = clean.dt.floor("D")
        mode_dates = normalized.mode()
        if mode_dates.empty:
            return clean.max()
        selected_day = mode_dates.max()
        same_day = clean[normalized == selected_day]
        return same_day.max() if not same_day.empty else clean.max()
    raise ValueError(f"Unsupported transaction date strategy: {strategy}")


def build_transactions(clean_df: pd.DataFrame, config: AppConfig) -> TransactionBuildResult:
    df = clean_df.copy()
    separator = config.transaction.id_separator
    df["transaction_id"] = df["external_order"].astype("string") + separator + df["owner"].astype("string")

    tx_item_df = (
        df.groupby(["transaction_id", "external_order", "owner", "article"], dropna=False)
        .agg(
            quantity_sum=("quantity", "sum"),
            line_count=("article", "size"),
            article_description=("article_description", "first"),
            primary_location=("location", "first"),
            unique_locations_for_sku=("location", lambda s: int(s.dropna().nunique())),
            first_completion_date=("completion_date", "min"),
            last_completion_date=("completion_date", "max"),
        )
        .reset_index()
    )

    transaction_dates = df.groupby("transaction_id")["completion_date"].apply(
        lambda s: _resolve_transaction_date(s, config.transaction.date_strategy)
    )
    basket_locations = df.assign(location_key=df["location"].fillna("[missing]")).groupby("transaction_id")["location_key"].nunique()

    transactions_df = (
        tx_item_df.groupby(["transaction_id", "external_order", "owner"], dropna=False)
        .agg(
            basket_size=("article", "nunique"),
            basket_total_quantity=("quantity_sum", "sum"),
            repeated_sku_lines=("line_count", lambda s: int((s > 1).sum())),
            transaction_start=("first_completion_date", "min"),
            transaction_end=("last_completion_date", "max"),
        )
        .reset_index()
        .merge(transaction_dates.rename("transaction_date").reset_index(), on="transaction_id", how="left")
        .merge(basket_locations.rename("unique_locations_in_basket").reset_index(), on="transaction_id", how="left")
    )
    transactions_df["repeated_sku_flag"] = transactions_df["repeated_sku_lines"] > 0
    transactions_df["basket_dispersion_proxy"] = transactions_df.apply(
        lambda row: safe_div(row["unique_locations_in_basket"], row["basket_size"]),
        axis=1,
    )
    transactions_df["year"] = transactions_df["transaction_date"].dt.year.astype("Int64")
    transactions_df["quarter"] = transactions_df["transaction_date"].dt.to_period("Q").astype("string")
    transactions_df["month"] = transactions_df["transaction_date"].dt.to_period("M").astype("string")

    transaction_item_sets = tx_item_df.groupby("transaction_id")["article"].agg(lambda s: tuple(sorted(set(s.astype(str)))))
    transaction_quantity_maps = tx_item_df.groupby("transaction_id")[["article", "quantity_sum"]].apply(
        lambda frame: dict(zip(frame["article"].astype(str), frame["quantity_sum"].astype(float)))
    )

    return TransactionBuildResult(
        tx_item_df=tx_item_df,
        transactions_df=transactions_df,
        transaction_item_sets=transaction_item_sets,
        transaction_quantity_maps=transaction_quantity_maps,
    )
