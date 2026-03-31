from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .config import AppConfig
from .utils import log_scale, minmax_scale


@dataclass
class ScoringResult:
    scored_pairs: pd.DataFrame
    score_metadata: dict[str, object]


def compute_layout_scores(pair_metrics: pd.DataFrame, stability_metrics: pd.DataFrame, config: AppConfig) -> ScoringResult:
    if pair_metrics.empty:
        return ScoringResult(scored_pairs=pair_metrics.copy(), score_metadata={})

    scored = pair_metrics.merge(stability_metrics, on=["article_a", "article_b"], how="left")
    scored["temporal_stability_score"] = scored["temporal_stability_score"].fillna(0)
    scored["presence_ratio"] = scored["presence_ratio"].fillna(0)

    scored["joint_frequency_component"] = log_scale(scored["shared_transactions"])
    scored["lift_component"] = minmax_scale(
        pd.Series(np.log1p((scored["lift"] - 1).clip(lower=0)), index=scored.index),
        cap_quantile=0.90,
    )
    scored["balanced_confidence_component"] = minmax_scale(scored["balanced_confidence"])
    scored["similarity_component"] = (
        scored["jaccard_similarity"].fillna(0)
        + scored["cosine_similarity"].fillna(0)
        + scored["weighted_cosine_similarity"].fillna(0).clip(upper=1)
    ) / 3
    scored["weighted_volume_component"] = log_scale(scored["weighted_shared_quantity"])
    scored["temporal_stability_component"] = scored["temporal_stability_score"].clip(lower=0, upper=1)

    weights = config.model.score_weights
    weighted_sum = (
        weights["joint_frequency"] * scored["joint_frequency_component"]
        + weights["lift"] * scored["lift_component"]
        + weights["balanced_confidence"] * scored["balanced_confidence_component"]
        + weights["similarity"] * scored["similarity_component"]
        + weights["temporal_stability"] * scored["temporal_stability_component"]
        + weights["weighted_volume"] * scored["weighted_volume_component"]
    )

    operational_floor = max(
        int(pair_metrics["shared_transactions"].quantile(0.99)),
        int(pair_metrics["shared_transactions"].min()),
        1,
    )
    recurrence_penalty = (scored["shared_transactions"] / operational_floor).clip(
        lower=config.model.recurrence_penalty_floor,
        upper=1.0,
    )
    stability_gate = 0.4 + 0.6 * scored["presence_ratio"].clip(lower=0, upper=1)
    scored["operational_relevance_factor"] = recurrence_penalty * stability_gate

    scored["final_layout_score"] = (
        weighted_sum
        * scored["operational_relevance_factor"]
        * scored["popularity_penalty_factor"].clip(lower=0.1, upper=1.0)
    ).clip(lower=0, upper=1)
    scored["proximity_recommendation"] = pd.cut(
        scored["final_layout_score"],
        bins=[-0.01, 0.25, 0.45, 0.65, 1.0],
        labels=["low", "medium", "high", "very_high"],
    ).astype("string")

    scored = scored.sort_values(
        ["final_layout_score", "shared_transactions", "temporal_stability_score"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    metadata = {
        "formula": (
            "score = (w_freq*freq_norm + w_lift*lift_norm + w_conf*balanced_conf_norm + "
            "w_similarity*similarity + w_stability*stability + w_volume*volume_norm) * "
            "operational_relevance_factor * popularity_penalty"
        ),
        "weights": weights,
        "operational_relevance_floor_transactions": operational_floor,
    }

    return ScoringResult(scored_pairs=scored, score_metadata=metadata)
