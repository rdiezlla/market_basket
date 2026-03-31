from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class TransactionBuildResult:
    tx_item_df: pd.DataFrame
    transactions_df: pd.DataFrame
    transaction_item_sets: pd.Series
    transaction_quantity_maps: pd.Series


def build_transactions(clean_df: pd.DataFrame) -> TransactionBuildResult:
    df = clean_df.copy()
    df["transaction_id"] = df["external_order"].astype("string") + "|" + df["owner"].astype("string")

    tx_item_df = (
        df.groupby(["transaction_id", "external_order", "owner", "article"], dropna=False)
        .agg(
            quantity_sum=("quantity", "sum"),
            line_count=("article", "size"),
            article_description=("article_description", "first"),
            primary_location=("location", "first"),
            first_completion_date=("completion_date", "min"),
            last_completion_date=("completion_date", "max"),
        )
        .reset_index()
    )

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
    )
    transactions_df["transaction_date"] = transactions_df["transaction_end"]
    transactions_df["year"] = transactions_df["transaction_date"].dt.year.astype("Int64")
    transactions_df["quarter"] = transactions_df["transaction_date"].dt.to_period("Q").astype("string")
    transactions_df["month"] = transactions_df["transaction_date"].dt.to_period("M").astype("string")

    transaction_item_sets = tx_item_df.groupby("transaction_id")["article"].agg(lambda s: tuple(sorted(set(s.astype(str)))))
    transaction_quantity_maps = tx_item_df.groupby("transaction_id")[["article", "quantity_sum"]].apply(
        lambda x: dict(zip(x["article"].astype(str), x["quantity_sum"].astype(float)))
    )

    return TransactionBuildResult(
        tx_item_df=tx_item_df,
        transactions_df=transactions_df,
        transaction_item_sets=transaction_item_sets,
        transaction_quantity_maps=transaction_quantity_maps,
    )
