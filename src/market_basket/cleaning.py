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
    sku_location_profile: pd.DataFrame
    profile: dict[str, float | int | str | None]
    exclusions_applied: dict[str, object]


def _add_quality_record(
    quality_records: list[dict],
    total_rows: int,
    issue: str,
    count: int,
    severity: str,
    stage: str,
    description: str,
    rule: str | None = None,
) -> None:
    quality_records.append(
        {
            "issue": issue,
            "count": int(count),
            "pct_total_rows": round(safe_div(count, total_rows) * 100, 4),
            "severity": severity,
            "stage": stage,
            "rule": rule,
            "description": description,
        }
    )


def _build_sku_profiles(model_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if model_df.empty:
        empty_location_profile = pd.DataFrame(
            columns=[
                "article",
                "location",
                "observations",
                "pct_of_article_rows",
                "is_primary_location",
                "latest_seen_at_location",
            ]
        )
        empty_sku_attributes = pd.DataFrame(
            columns=[
                "article",
                "article_description",
                "primary_location",
                "location_count",
                "description_count",
                "dominant_location_share",
                "latest_location",
                "multi_location_flag",
            ]
        )
        return empty_location_profile, empty_sku_attributes

    profile_base = model_df.copy()
    profile_base["location_profile_key"] = profile_base["location"].fillna("[missing]")

    article_row_counts = profile_base.groupby("article").size().rename("article_row_count")
    latest_location = (
        profile_base.sort_values(["article", "completion_date"])
        .groupby("article", dropna=False)["location_profile_key"]
        .last()
        .rename("latest_location")
    )

    sku_location_profile = (
        profile_base.groupby(["article", "location_profile_key"], dropna=False)
        .agg(
            observations=("location_profile_key", "size"),
            latest_seen_at_location=("completion_date", "max"),
        )
        .reset_index()
        .rename(columns={"location_profile_key": "location"})
        .merge(article_row_counts.reset_index(), on="article", how="left")
    )
    sku_location_profile["pct_of_article_rows"] = sku_location_profile["observations"] / sku_location_profile["article_row_count"].clip(lower=1)
    sku_location_profile = sku_location_profile.sort_values(
        ["article", "observations", "latest_seen_at_location", "location"],
        ascending=[True, False, False, True],
    ).reset_index(drop=True)
    sku_location_profile["is_primary_location"] = (
        sku_location_profile.groupby("article").cumcount() == 0
    )
    sku_location_profile = sku_location_profile[
        [
            "article",
            "location",
            "observations",
            "pct_of_article_rows",
            "is_primary_location",
            "latest_seen_at_location",
        ]
    ]

    primary_location = (
        sku_location_profile.loc[sku_location_profile["is_primary_location"], ["article", "location", "pct_of_article_rows"]]
        .rename(columns={"location": "primary_location", "pct_of_article_rows": "dominant_location_share"})
    )

    sku_attributes = (
        model_df.groupby("article", dropna=False)
        .agg(
            article_description=("article_description", stable_mode),
            location_count=("location", lambda s: int(s.dropna().nunique())),
            description_count=("article_description", lambda s: int(s.dropna().nunique())),
        )
        .reset_index()
        .merge(primary_location, on="article", how="left")
        .merge(latest_location.reset_index(), on="article", how="left")
    )
    sku_attributes["multi_location_flag"] = sku_attributes["location_count"] > 1
    sku_attributes["dominant_location_share"] = sku_attributes["dominant_location_share"].fillna(0.0)

    return sku_location_profile, sku_attributes


def clean_movements(raw_df: pd.DataFrame, config: AppConfig) -> CleanResult:
    df = raw_df.copy()
    total_rows = len(df)

    df["completion_date"] = pd.to_datetime(df["completion_date"], errors="coerce", dayfirst=True)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

    quality_records: list[dict] = []
    null_summary = (
        df.isna()
        .sum()
        .rename("null_count")
        .reset_index()
        .rename(columns={"index": "column"})
        .assign(pct_null=lambda x: (x["null_count"] / max(len(df), 1)).round(6))
    )

    _add_quality_record(quality_records, total_rows, "missing_completion_date", int(df["completion_date"].isna().sum()), "high", "raw", "Rows without a parseable completion date.")
    _add_quality_record(quality_records, total_rows, "missing_article", int(df["article"].isna().sum()), "high", "raw", "Rows without article.")
    _add_quality_record(quality_records, total_rows, "missing_external_order", int(df["external_order"].isna().sum()), "high", "raw", "Rows without external order.")
    _add_quality_record(quality_records, total_rows, "missing_owner", int(df["owner"].isna().sum()), "high", "raw", "Rows without owner.")
    _add_quality_record(quality_records, total_rows, "missing_location", int(df["location"].isna().sum()), "medium", "raw", "Rows without location.")
    _add_quality_record(quality_records, total_rows, "non_positive_quantity_raw", int((df["quantity"].fillna(0) <= 0).sum()), "medium", "raw", "Rows with null, zero or negative quantity.")

    duplicate_subset = list(config.data_quality.duplicate_subset)
    duplicate_candidates = df.duplicated(subset=duplicate_subset, keep=False)
    _add_quality_record(
        quality_records,
        total_rows,
        "potential_duplicate_rows",
        int(duplicate_candidates.sum()),
        "medium",
        "raw",
        "Potential duplicates detected over the configured subset.",
        rule=",".join(duplicate_subset),
    )

    rows_before_dedup = len(df)
    if config.data_quality.drop_exact_duplicates:
        df = df.drop_duplicates(subset=duplicate_subset, keep="first").copy()
        removed = rows_before_dedup - len(df)
        _add_quality_record(
            quality_records,
            total_rows,
            "exact_duplicates_removed",
            removed,
            "info",
            "raw",
            "Exact duplicates removed before movement filtering.",
            rule=",".join(duplicate_subset),
        )

    pi_df = df[df["movement_type"] == config.model.valid_movement_type].copy()
    _add_quality_record(
        quality_records,
        total_rows,
        "rows_after_movement_filter",
        len(pi_df),
        "info",
        "filtered",
        f"Rows kept after filtering movement_type = {config.model.valid_movement_type}.",
        rule=config.model.valid_movement_type,
    )

    excluded_missing_order = pi_df[pi_df["external_order"].isna()].copy()
    _add_quality_record(
        quality_records,
        total_rows,
        "rows_excluded_missing_external_order_in_pi",
        len(excluded_missing_order),
        "high",
        "filtered",
        "PI rows excluded from the main model due to missing external order.",
    )

    non_positive_quantity_in_pi = pi_df[pi_df["quantity"].fillna(0) <= 0].copy()
    if config.data_quality.exclude_non_positive_quantity:
        _add_quality_record(
            quality_records,
            total_rows,
            "rows_excluded_non_positive_quantity_in_pi",
            len(non_positive_quantity_in_pi),
            "high",
            "filtered",
            "PI rows excluded from the main model due to quantity <= 0.",
            rule="data_quality.exclude_non_positive_quantity=true",
        )

    model_mask = (
        pi_df["article"].notna()
        & pi_df["owner"].notna()
        & pi_df["external_order"].notna()
        & pi_df["completion_date"].notna()
        & pi_df["quantity"].notna()
    )
    if config.data_quality.exclude_non_positive_quantity:
        model_mask &= pi_df["quantity"] > 0

    model_df = pi_df.loc[model_mask].copy()
    model_df["article_description"] = model_df["article_description"].fillna("")
    model_df["year"] = model_df["completion_date"].dt.year.astype("Int64")
    model_df["quarter"] = model_df["completion_date"].dt.to_period("Q").astype("string")
    model_df["month"] = model_df["completion_date"].dt.to_period("M").astype("string")

    sku_location_profile, sku_attributes = _build_sku_profiles(model_df)

    _add_quality_record(
        quality_records,
        total_rows,
        "rows_after_model_filters",
        len(model_df),
        "info",
        "model",
        "Rows available for the main basket model after cleaning and business filters.",
    )

    exclusions_applied = {
        "movement_type_filter": config.model.valid_movement_type,
        "exclude_missing_external_order": True,
        "exclude_non_positive_quantity": config.data_quality.exclude_non_positive_quantity,
        "drop_exact_duplicates": config.data_quality.drop_exact_duplicates,
        "duplicate_subset": duplicate_subset,
        "excluded_missing_external_order_rows": int(len(excluded_missing_order)),
        "excluded_non_positive_quantity_rows": int(len(non_positive_quantity_in_pi)) if config.data_quality.exclude_non_positive_quantity else 0,
        "removed_exact_duplicates": int(rows_before_dedup - len(df)) if config.data_quality.drop_exact_duplicates else 0,
    }

    profile = {
        "rows_total": int(total_rows),
        "rows_pi": int(len(pi_df)),
        "rows_model": int(len(model_df)),
        "min_date": model_df["completion_date"].min().isoformat() if not model_df.empty else None,
        "max_date": model_df["completion_date"].max().isoformat() if not model_df.empty else None,
        "unique_articles": int(model_df["article"].nunique()),
        "unique_owners": int(model_df["owner"].nunique()),
        "missing_order_in_pi": int(len(excluded_missing_order)),
        "non_positive_quantity_in_pi": int(len(non_positive_quantity_in_pi)),
    }

    quality_summary = (
        pd.DataFrame(quality_records)
        .sort_values(["stage", "severity", "issue"], ascending=[True, True, True])
        .reset_index(drop=True)
    )

    return CleanResult(
        clean_df=model_df,
        quality_summary=quality_summary,
        null_summary=null_summary,
        excluded_missing_order=excluded_missing_order,
        sku_attributes=sku_attributes,
        sku_location_profile=sku_location_profile,
        profile=profile,
        exclusions_applied=exclusions_applied,
    )
