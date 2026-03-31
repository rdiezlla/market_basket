from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .associations import compute_associations
from .config import AppConfig
from .utils import bounded_inverse_cv


@dataclass
class TemporalResult:
    temporal_pairs: pd.DataFrame
    stability_metrics: pd.DataFrame


def compute_temporal_stability(tx_item_df: pd.DataFrame, config: AppConfig) -> TemporalResult:
    periods: list[tuple[str, pd.DataFrame]] = []
    max_date = tx_item_df["last_completion_date"].max()

    periods.append(("full_history", tx_item_df.copy()))
    for days in config.temporal.rolling_windows_days:
        start_date = max_date - pd.Timedelta(days=days)
        periods.append((f"last_{days}_days", tx_item_df[tx_item_df["last_completion_date"] >= start_date].copy()))

    if config.temporal.include_yearly:
        for year, group in tx_item_df.groupby(tx_item_df["last_completion_date"].dt.year):
            periods.append((f"year_{int(year)}", group.copy()))

    if config.temporal.include_quarterly:
        for quarter, group in tx_item_df.groupby(tx_item_df["last_completion_date"].dt.to_period("Q")):
            periods.append((f"quarter_{quarter}", group.copy()))

    temporal_frames: list[pd.DataFrame] = []
    for label, frame in periods:
        if frame["transaction_id"].nunique() < 2:
            continue
        result = compute_associations(frame, config)
        if result.pair_metrics.empty:
            continue
        enriched = result.pair_metrics.copy()
        enriched["period_label"] = label
        enriched["period_transaction_count"] = int(frame["transaction_id"].nunique())
        temporal_frames.append(
            enriched[
                [
                    "article_a",
                    "article_b",
                    "period_label",
                    "period_transaction_count",
                    "shared_transactions",
                    "joint_support",
                    "lift",
                    "balanced_confidence",
                    "jaccard_similarity",
                    "cosine_similarity",
                ]
            ]
        )

    temporal_pairs = pd.concat(temporal_frames, ignore_index=True) if temporal_frames else pd.DataFrame()
    if temporal_pairs.empty:
        return TemporalResult(temporal_pairs=temporal_pairs, stability_metrics=pd.DataFrame())

    support_stability = temporal_pairs.groupby(["article_a", "article_b"], dropna=False)["joint_support"].apply(bounded_inverse_cv).rename("support_stability")
    lift_stability = temporal_pairs.groupby(["article_a", "article_b"], dropna=False)["lift"].apply(bounded_inverse_cv).rename("lift_stability")
    stability_metrics = (
        temporal_pairs.groupby(["article_a", "article_b"], dropna=False)
        .agg(
            periods_present=("period_label", "nunique"),
            support_mean=("joint_support", "mean"),
            support_std=("joint_support", "std"),
            lift_mean=("lift", "mean"),
            lift_std=("lift", "std"),
            balanced_confidence_mean=("balanced_confidence", "mean"),
            shared_transactions_mean=("shared_transactions", "mean"),
        )
        .reset_index()
        .merge(support_stability.reset_index(), on=["article_a", "article_b"], how="left")
        .merge(lift_stability.reset_index(), on=["article_a", "article_b"], how="left")
    )
    available_periods = temporal_pairs["period_label"].nunique()
    stability_metrics["periods_available"] = available_periods
    stability_metrics["presence_ratio"] = stability_metrics["periods_present"] / max(available_periods, 1)
    stability_metrics["temporal_stability_score"] = (
        0.45 * stability_metrics["presence_ratio"]
        + 0.30 * stability_metrics["support_stability"].fillna(0)
        + 0.25 * stability_metrics["lift_stability"].fillna(0)
    )
    stability_metrics = stability_metrics.sort_values(
        ["temporal_stability_score", "periods_present", "support_mean"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    return TemporalResult(temporal_pairs=temporal_pairs, stability_metrics=stability_metrics)
