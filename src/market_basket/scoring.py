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


def _build_layout_hints(scored: pd.DataFrame, bins: list[float]) -> pd.DataFrame:
    high_bin = bins[-1]
    medium_bin = bins[-2] if len(bins) >= 2 else bins[-1]

    scored["candidate_same_slot_area"] = (
        (scored["final_layout_score"] >= high_bin)
        & (scored["shared_transactions"] >= scored["shared_transactions"].median())
        & (scored["temporal_stability_score"] >= 0.55)
    )
    scored["candidate_same_zone"] = (
        (scored["final_layout_score"] >= medium_bin)
        & ~scored["candidate_same_slot_area"]
    )
    scored["candidate_manual_review"] = (
        (scored["lift_component"] >= 0.70) & (scored["joint_frequency_component"] < 0.35)
    ) | (
        (scored["final_layout_score"] >= medium_bin) & (scored["presence_ratio"] < 0.40)
    )

    action_hints = np.select(
        [
            scored["candidate_same_slot_area"],
            scored["candidate_same_zone"],
            scored["candidate_manual_review"],
        ],
        [
            "Priorizar cercania fisica fuerte dentro de la misma area o frente de picking.",
            "Mantener dentro de la misma zona operativa y revisar secuencia de recorrido.",
            "Revisar manualmente: relacion prometedora pero menos consolidada o potencialmente espuria.",
        ],
        default="Sin accion inmediata; monitorizar o mantener separado salvo criterio operativo adicional.",
    )
    scored["layout_action_hint"] = pd.Series(action_hints, index=scored.index, dtype="string")
    return scored


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
        + scored["npmi"].fillna(0).clip(lower=0, upper=1)
    ) / 4
    scored["weighted_volume_component"] = log_scale(scored["weighted_shared_quantity"])
    scored["temporal_stability_component"] = scored["temporal_stability_score"].clip(lower=0, upper=1)

    weights = config.model.score_weights
    scored["weighted_score_pre_penalty"] = (
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
        scored["weighted_score_pre_penalty"]
        * scored["operational_relevance_factor"]
        * scored["popularity_penalty_factor"].clip(lower=0.1, upper=1.0)
    ).clip(lower=0, upper=1)

    bins = list(config.thresholds.scoring.proximity_bins)
    labels = list(config.thresholds.scoring.proximity_labels)
    scored["proximity_recommendation"] = pd.cut(
        scored["final_layout_score"],
        bins=[-0.001, *bins, 1.0],
        labels=labels,
        include_lowest=True,
    ).astype("string")
    scored = _build_layout_hints(scored, bins)

    scored = scored.sort_values(
        ["final_layout_score", "shared_transactions", "temporal_stability_score"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    metadata = {
        "formula": (
            "score = weighted_score_pre_penalty * operational_relevance_factor * popularity_penalty"
        ),
        "components": {
            "joint_frequency_component": "Frecuencia conjunta normalizada con escala logaritmica.",
            "lift_component": "Lift suavizado con log1p para reducir el impacto de extremos raros.",
            "balanced_confidence_component": "Confianza bidireccional equilibrada.",
            "similarity_component": "Promedio de Jaccard, cosine, weighted cosine y npmi positivo.",
            "weighted_volume_component": "Volumen compartido ponderado por cantidad.",
            "temporal_stability_component": "Persistencia y estabilidad temporal de la relacion.",
            "operational_relevance_factor": "Penaliza baja recurrencia y baja presencia temporal.",
            "popularity_penalty_factor": "Reduce el peso de relaciones triviales dominadas por SKUs muy frecuentes.",
        },
        "weights": weights,
        "recommendation_bins": {
            "bins": bins,
            "labels": labels,
        },
        "operational_relevance_floor_transactions": operational_floor,
    }

    return ScoringResult(scored_pairs=scored, score_metadata=metadata)
