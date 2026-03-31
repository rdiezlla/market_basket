from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .config import AppConfig
from .utils import safe_div, stable_mode


@dataclass
class CleanResult:
    clean_df: pd.DataFrame
    quality_summary: pd.DataFrame
    null_summary: pd.DataFrame
    excluded_missing_order: pd.DataFrame
    sku_attributes: pd.DataFrame
    profile: dict[str, float | int | str | None]


def clean_movements(raw_df: pd.DataFrame, config: AppConfig) -> CleanResult:
    df = raw_df.copy()
    original_rows = len(df)

    df["completion_date"] = pd.to_datetime(df["completion_date"], errors="coerce", dayfirst=True)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

    quality_records: list[dict] = []

    def add_quality(issue: str, count: int, severity: str, stage: str, description: str) -> None:
        quality_records.append(
            {
                "issue": issue,
                "count": int(count),
                "pct_total_rows": round(safe_div(count, original_rows) * 100, 4),
                "severity": severity,
                "stage": stage,
                "description": description,
            }
        )

    null_summary = (
        df.isna()
        .sum()
        .rename("null_count")
        .reset_index()
        .rename(columns={"index": "column"})
        .assign(pct_null=lambda x: (x["null_count"] / max(len(df), 1)).round(6))
    )

    add_quality("missing_completion_date", int(df["completion_date"].isna().sum()), "high", "raw", "Filas sin fecha finalización interpretable.")
    add_quality("missing_article", int(df["article"].isna().sum()), "high", "raw", "Filas sin artículo.")
    add_quality("missing_external_order", int(df["external_order"].isna().sum()), "high", "raw", "Filas sin pedido externo.")
    add_quality("missing_owner", int(df["owner"].isna().sum()), "high", "raw", "Filas sin propietario.")
    add_quality("missing_location", int(df["location"].isna().sum()), "medium", "raw", "Filas sin ubicación.")
    add_quality("non_positive_quantity", int((df["quantity"].fillna(0) <= 0).sum()), "medium", "raw", "Filas con cantidad nula o no positiva.")

    duplicated_mask = df.duplicated(
        subset=["movement_type", "completion_date", "article", "quantity", "owner", "location", "external_order"],
        keep=False,
    )
    add_quality("potential_duplicate_rows", int(duplicated_mask.sum()), "medium", "raw", "Filas duplicadas sobre la clave operativa mínima.")

    pi_df = df[df["movement_type"] == config.model.valid_movement_type].copy()
    add_quality(
        "rows_after_movement_filter",
        int(len(pi_df)),
        "info",
        "filtered",
        f"Filas conservadas tras filtrar Tipo movimiento = {config.model.valid_movement_type}.",
    )

    excluded_missing_order = pi_df[pi_df["external_order"].isna()].copy()
    model_df = pi_df[
        pi_df["article"].notna()
        & pi_df["owner"].notna()
        & pi_df["external_order"].notna()
        & pi_df["completion_date"].notna()
        & pi_df["quantity"].notna()
    ].copy()

    model_df["article_description"] = model_df["article_description"].fillna("")
    model_df["location"] = model_df["location"].fillna("")
    model_df["year"] = model_df["completion_date"].dt.year.astype("Int64")
    model_df["quarter"] = model_df["completion_date"].dt.to_period("Q").astype("string")
    model_df["month"] = model_df["completion_date"].dt.to_period("M").astype("string")

    add_quality(
        "rows_excluded_missing_external_order_in_pi",
        int(len(excluded_missing_order)),
        "high",
        "filtered",
        "Filas PI excluidas del modelo principal por no tener pedido externo.",
    )
    add_quality("rows_after_model_filters", int(len(model_df)), "info", "model", "Filas válidas para el modelo principal.")

    sku_attributes = (
        model_df.groupby("article", dropna=False)
        .agg(
            article_description=("article_description", stable_mode),
            primary_location=("location", stable_mode),
            location_count=("location", lambda s: int(s.replace("", pd.NA).dropna().nunique())),
            description_count=("article_description", lambda s: int(s.replace("", pd.NA).dropna().nunique())),
        )
        .reset_index()
    )

    profile = {
        "rows_total": int(original_rows),
        "rows_pi": int(len(pi_df)),
        "rows_model": int(len(model_df)),
        "min_date": model_df["completion_date"].min().isoformat() if not model_df.empty else None,
        "max_date": model_df["completion_date"].max().isoformat() if not model_df.empty else None,
        "unique_articles": int(model_df["article"].nunique()),
        "unique_owners": int(model_df["owner"].nunique()),
        "missing_order_in_pi": int(len(excluded_missing_order)),
    }

    quality_summary = pd.DataFrame(quality_records).sort_values(["stage", "severity", "issue"]).reset_index(drop=True)

    return CleanResult(
        clean_df=model_df,
        quality_summary=quality_summary,
        null_summary=null_summary,
        excluded_missing_order=excluded_missing_order,
        sku_attributes=sku_attributes,
        profile=profile,
    )
