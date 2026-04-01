from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .associations import compute_associations
from .config import AppConfig
from .utils import bounded_inverse_cv


@dataclass
class TemporalResult:
    raw_temporal_pairs: pd.DataFrame
    temporal_pairs: pd.DataFrame
    stability_metrics: pd.DataFrame


def _trend_label(slope: float, mean_value: float) -> str:
    tolerance = max(abs(mean_value) * 0.05, 1e-6)
    if slope > tolerance:
        return "growing"
    if slope < -tolerance:
        return "declining"
    return "stable"


def _build_periods(tx_item_df: pd.DataFrame, config: AppConfig) -> list[tuple[str, str, pd.Timestamp, pd.Timestamp, pd.DataFrame]]:
    periods: list[tuple[str, str, pd.Timestamp, pd.Timestamp, pd.DataFrame]] = []
    max_date = tx_item_df["last_completion_date"].max()
    min_date = tx_item_df["last_completion_date"].min()

    periods.append(("full_history", "full_history", min_date, max_date, tx_item_df.copy()))
    for days in config.temporal.rolling_windows_days:
        start_date = max_date - pd.Timedelta(days=days)
        frame = tx_item_df[tx_item_df["last_completion_date"] >= start_date].copy()
        periods.append((f"last_{days}_days", "rolling_window", start_date, max_date, frame))

    if config.temporal.include_yearly:
        for year, group in tx_item_df.groupby(tx_item_df["last_completion_date"].dt.year):
            start = pd.Timestamp(year=int(year), month=1, day=1)
            end = start + pd.offsets.YearEnd(0)
            periods.append((f"year_{int(year)}", "year", start, end, group.copy()))

    if config.temporal.include_quarterly:
        for quarter, group in tx_item_df.groupby(tx_item_df["last_completion_date"].dt.to_period("Q")):
            periods.append((f"quarter_{quarter}", "quarter", quarter.start_time, quarter.end_time, group.copy()))

    return periods


def _compute_raw_temporal_pairs(tx_item_df: pd.DataFrame, config: AppConfig) -> pd.DataFrame:
    min_period_transactions = config.performance.raw_temporal_min_period_transactions
    raw_min_shared = config.thresholds.pairs.raw_temporal_min_shared_transactions

    frames: list[pd.DataFrame] = []
    for period_order, (label, granularity, period_start, period_end, frame) in enumerate(_build_periods(tx_item_df, config)):
        transaction_count = int(frame["transaction_id"].nunique())
        if transaction_count < min_period_transactions:
            continue

        period_result = compute_associations(
            frame,
            config,
            min_pair_transactions_override=raw_min_shared,
        )
        if period_result.pair_metrics.empty:
            continue

        enriched = period_result.pair_metrics.copy()
        enriched["period_label"] = label
        enriched["period_granularity"] = granularity
        enriched["period_start"] = period_start
        enriched["period_end"] = period_end
        enriched["period_sort_key"] = period_start
        enriched["period_order"] = period_order
        enriched["period_transaction_count"] = transaction_count
        frames.append(
            enriched[
                [
                    "article_a",
                    "article_b",
                    "period_label",
                    "period_granularity",
                    "period_start",
                    "period_end",
                    "period_sort_key",
                    "period_order",
                    "period_transaction_count",
                    "shared_transactions",
                    "joint_support",
                    "lift",
                    "balanced_confidence",
                    "jaccard_similarity",
                    "cosine_similarity",
                    "npmi",
                    "residual_cooccurrence",
                ]
            ]
        )

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values(["period_sort_key", "article_a", "article_b"]).reset_index(drop=True)


def _compute_trend_metrics(raw_temporal_pairs: pd.DataFrame) -> pd.DataFrame:
    trend_rows: list[dict] = []
    trend_source = raw_temporal_pairs[raw_temporal_pairs["period_granularity"] != "full_history"].copy()

    for (article_a, article_b), frame in trend_source.groupby(["article_a", "article_b"], dropna=False):
        ordered = frame.sort_values("period_sort_key").reset_index(drop=True)
        x = np.arange(len(ordered), dtype=float)
        support_values = ordered["joint_support"].astype(float).values
        lift_values = ordered["lift"].astype(float).values

        if len(ordered) >= 2:
            support_slope = float(np.polyfit(x, support_values, 1)[0])
            lift_slope = float(np.polyfit(x, lift_values, 1)[0])
        else:
            support_slope = 0.0
            lift_slope = 0.0

        support_mean = float(np.mean(support_values)) if len(support_values) else 0.0
        lift_mean = float(np.mean(lift_values)) if len(lift_values) else 0.0
        support_trend = _trend_label(support_slope, support_mean)
        lift_trend = _trend_label(lift_slope, lift_mean)
        overall_trend = support_trend if support_trend == lift_trend else "stable"

        trend_rows.append(
            {
                "article_a": article_a,
                "article_b": article_b,
                "support_slope": support_slope,
                "lift_slope": lift_slope,
                "support_trend": support_trend,
                "lift_trend": lift_trend,
                "trend_classification": overall_trend,
            }
        )

    return pd.DataFrame(trend_rows)


def compute_temporal_stability(tx_item_df: pd.DataFrame, config: AppConfig) -> TemporalResult:
    raw_temporal_pairs = _compute_raw_temporal_pairs(tx_item_df, config)
    if raw_temporal_pairs.empty:
        empty = pd.DataFrame()
        return TemporalResult(raw_temporal_pairs=empty, temporal_pairs=empty, stability_metrics=empty)

    support_stability = (
        raw_temporal_pairs.groupby(["article_a", "article_b"], dropna=False)["joint_support"]
        .apply(bounded_inverse_cv)
        .rename("support_stability")
    )
    lift_stability = (
        raw_temporal_pairs.groupby(["article_a", "article_b"], dropna=False)["lift"]
        .apply(bounded_inverse_cv)
        .rename("lift_stability")
    )
    trend_metrics = _compute_trend_metrics(raw_temporal_pairs)

    stability_metrics = (
        raw_temporal_pairs.groupby(["article_a", "article_b"], dropna=False)
        .agg(
            periods_present=("period_label", "nunique"),
            support_mean=("joint_support", "mean"),
            support_std=("joint_support", "std"),
            support_max=("joint_support", "max"),
            lift_mean=("lift", "mean"),
            lift_std=("lift", "std"),
            balanced_confidence_mean=("balanced_confidence", "mean"),
            shared_transactions_mean=("shared_transactions", "mean"),
            npmi_mean=("npmi", "mean"),
        )
        .reset_index()
        .merge(support_stability.reset_index(), on=["article_a", "article_b"], how="left")
        .merge(lift_stability.reset_index(), on=["article_a", "article_b"], how="left")
        .merge(trend_metrics, on=["article_a", "article_b"], how="left")
    )

    available_periods = raw_temporal_pairs["period_label"].nunique()
    stability_metrics["periods_available"] = available_periods
    stability_metrics["presence_ratio"] = stability_metrics["periods_present"] / max(available_periods, 1)
    stability_metrics["temporal_stability_score"] = (
        0.40 * stability_metrics["presence_ratio"]
        + 0.25 * stability_metrics["support_stability"].fillna(0)
        + 0.20 * stability_metrics["lift_stability"].fillna(0)
        + 0.15 * (stability_metrics["support_trend"].fillna("stable") != "declining").astype(float)
    )
    stability_metrics = stability_metrics.sort_values(
        ["temporal_stability_score", "periods_present", "support_mean"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    return TemporalResult(
        raw_temporal_pairs=raw_temporal_pairs,
        temporal_pairs=raw_temporal_pairs.copy(),
        stability_metrics=stability_metrics,
    )
