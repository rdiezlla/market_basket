from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class EDAResult:
    kpi_summary: pd.DataFrame
    article_summary: pd.DataFrame
    transaction_summary: pd.DataFrame
    owner_article_summary: pd.DataFrame
    time_series: pd.DataFrame


def build_eda_outputs(
    clean_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
    tx_item_df: pd.DataFrame,
    sku_attributes: pd.DataFrame,
) -> EDAResult:
    basket_distribution = transactions_df["basket_size"].value_counts().sort_index()

    kpi_summary = pd.DataFrame(
        [
            {
                "movimientos_pi": int(len(clean_df)),
                "transacciones_validas": int(transactions_df["transaction_id"].nunique()),
                "articulos_unicos": int(tx_item_df["article"].nunique()),
                "propietarios_unicos": int(transactions_df["owner"].nunique()),
                "tamano_cesta_media": round(float(transactions_df["basket_size"].mean()), 4),
                "tamano_cesta_mediana": round(float(transactions_df["basket_size"].median()), 4),
                "pct_transacciones_1_articulo": round(float((transactions_df["basket_size"] == 1).mean()), 4),
                "pct_transacciones_2_articulos": round(float((transactions_df["basket_size"] == 2).mean()), 4),
                "pct_transacciones_3_o_mas": round(float((transactions_df["basket_size"] >= 3).mean()), 4),
                "fecha_min": transactions_df["transaction_date"].min(),
                "fecha_max": transactions_df["transaction_date"].max(),
            }
        ]
    )

    article_summary = (
        tx_item_df.groupby("article", dropna=False)
        .agg(
            transaction_frequency=("transaction_id", "nunique"),
            row_frequency=("transaction_id", "size"),
            total_quantity=("quantity_sum", "sum"),
            mean_quantity_per_tx=("quantity_sum", "mean"),
            owner_count=("owner", "nunique"),
            first_seen=("last_completion_date", "min"),
            last_seen=("last_completion_date", "max"),
        )
        .reset_index()
        .merge(sku_attributes, on="article", how="left")
        .sort_values(["transaction_frequency", "total_quantity"], ascending=[False, False])
        .reset_index(drop=True)
    )

    owner_article_summary = (
        tx_item_df.groupby(["owner", "article"], dropna=False)
        .agg(transaction_frequency=("transaction_id", "nunique"), total_quantity=("quantity_sum", "sum"))
        .reset_index()
        .sort_values(["owner", "transaction_frequency", "total_quantity"], ascending=[True, False, False])
    )

    yearly = (
        transactions_df.groupby("year", dropna=False)
        .agg(transactions=("transaction_id", "nunique"), avg_basket_size=("basket_size", "mean"))
        .reset_index()
        .assign(granularity="year", period=lambda x: x["year"].astype("string"))
    )
    quarterly = (
        transactions_df.groupby("quarter", dropna=False)
        .agg(transactions=("transaction_id", "nunique"), avg_basket_size=("basket_size", "mean"))
        .reset_index()
        .assign(granularity="quarter", period=lambda x: x["quarter"].astype("string"))
    )
    monthly = (
        transactions_df.groupby("month", dropna=False)
        .agg(transactions=("transaction_id", "nunique"), avg_basket_size=("basket_size", "mean"))
        .reset_index()
        .assign(granularity="month", period=lambda x: x["month"].astype("string"))
    )
    basket_dist_df = basket_distribution.rename("transactions").reset_index()
    basket_dist_df.columns = ["period", "transactions"]
    basket_dist_df["granularity"] = "basket_size_distribution"
    basket_dist_df["avg_basket_size"] = float("nan")

    time_series = pd.concat(
        [
            yearly[["granularity", "period", "transactions", "avg_basket_size"]],
            quarterly[["granularity", "period", "transactions", "avg_basket_size"]],
            monthly[["granularity", "period", "transactions", "avg_basket_size"]],
            basket_dist_df[["granularity", "period", "transactions", "avg_basket_size"]],
        ],
        ignore_index=True,
    )

    return EDAResult(
        kpi_summary=kpi_summary,
        article_summary=article_summary,
        transaction_summary=transactions_df.sort_values("transaction_date").reset_index(drop=True),
        owner_article_summary=owner_article_summary,
        time_series=time_series,
    )
