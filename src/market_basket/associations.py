from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from itertools import combinations

import numpy as np
import pandas as pd

from .config import AppConfig
from .utils import harmonic_mean, safe_div


@dataclass
class AssociationResult:
    pair_metrics: pd.DataFrame
    rule_metrics: pd.DataFrame
    item_metrics: pd.DataFrame
    thresholds: dict[str, float | int]


def derive_thresholds(
    transaction_count: int,
    pair_counter: Counter[tuple[str, str]],
    config: AppConfig,
) -> dict[str, float | int]:
    if config.model.min_pair_transactions is not None:
        min_pair_transactions = int(config.model.min_pair_transactions)
    else:
        support_floor = config.model.min_support if config.model.min_support is not None else 0.0002
        support_driven_count = int(np.ceil(transaction_count * support_floor))
        pair_counts = pd.Series(pair_counter, dtype=float)
        quantile_count = int(max(3, min(5, pair_counts.quantile(0.95)))) if not pair_counts.empty else 1
        min_pair_transactions = max(3, support_driven_count, quantile_count)

    min_support = config.model.min_support
    if min_support is None:
        min_support = safe_div(min_pair_transactions, transaction_count)

    return {
        "min_pair_transactions": int(min_pair_transactions),
        "min_support": float(min_support),
        "min_confidence": float(config.model.min_confidence),
        "min_lift": float(config.model.min_lift),
    }


def compute_associations(tx_item_df: pd.DataFrame, config: AppConfig) -> AssociationResult:
    total_transactions = int(tx_item_df["transaction_id"].nunique())
    item_tx_counts = tx_item_df.groupby("article")["transaction_id"].nunique().sort_values(ascending=False)
    item_qty_totals = tx_item_df.groupby("article")["quantity_sum"].sum()

    pair_counter: Counter[tuple[str, str]] = Counter()
    weighted_pair_counter: Counter[tuple[str, str]] = Counter()

    for _, group in tx_item_df.groupby("transaction_id"):
        article_qty = dict(zip(group["article"].astype(str), group["quantity_sum"].astype(float)))
        basket = sorted(article_qty)
        if len(basket) < 2:
            continue
        for article_a, article_b in combinations(basket, 2):
            pair_counter[(article_a, article_b)] += 1
            weighted_pair_counter[(article_a, article_b)] += min(article_qty[article_a], article_qty[article_b])

    thresholds = derive_thresholds(total_transactions, pair_counter, config)

    pair_rows: list[dict] = []
    for (article_a, article_b), shared_transactions in pair_counter.items():
        if shared_transactions < int(thresholds["min_pair_transactions"]):
            continue

        weighted_shared_qty = weighted_pair_counter[(article_a, article_b)]
        support_a = safe_div(item_tx_counts.get(article_a, 0), total_transactions)
        support_b = safe_div(item_tx_counts.get(article_b, 0), total_transactions)
        joint_support = safe_div(shared_transactions, total_transactions)
        confidence_a_b = safe_div(shared_transactions, item_tx_counts.get(article_a, 0))
        confidence_b_a = safe_div(shared_transactions, item_tx_counts.get(article_b, 0))
        expected_support = support_a * support_b
        lift = safe_div(joint_support, expected_support)
        leverage = joint_support - expected_support
        conviction_a_b = np.inf if confidence_a_b >= 1 else safe_div(1 - support_b, 1 - confidence_a_b)
        conviction_b_a = np.inf if confidence_b_a >= 1 else safe_div(1 - support_a, 1 - confidence_b_a)
        jaccard = safe_div(shared_transactions, item_tx_counts.get(article_a, 0) + item_tx_counts.get(article_b, 0) - shared_transactions)
        cosine = safe_div(shared_transactions, np.sqrt(item_tx_counts.get(article_a, 0) * item_tx_counts.get(article_b, 0)))
        weighted_cosine = safe_div(weighted_shared_qty, np.sqrt(item_qty_totals.get(article_a, 0) * item_qty_totals.get(article_b, 0)))
        balanced_confidence = harmonic_mean([confidence_a_b, confidence_b_a])
        popularity_score = max(support_a, support_b)
        spurious_penalty = 1.0 / (1.0 + config.model.popularity_penalty_alpha * popularity_score)

        pair_rows.append(
            {
                "article_a": article_a,
                "article_b": article_b,
                "shared_transactions": int(shared_transactions),
                "joint_support": joint_support,
                "support_a": support_a,
                "support_b": support_b,
                "confidence_a_b": confidence_a_b,
                "confidence_b_a": confidence_b_a,
                "balanced_confidence": balanced_confidence,
                "lift": lift,
                "leverage": leverage,
                "conviction_a_b": conviction_a_b,
                "conviction_b_a": conviction_b_a,
                "jaccard_similarity": jaccard,
                "cosine_similarity": cosine,
                "weighted_shared_quantity": weighted_shared_qty,
                "weighted_cosine_similarity": weighted_cosine,
                "popularity_penalty_factor": spurious_penalty,
            }
        )

    pair_metrics = pd.DataFrame(pair_rows)
    if not pair_metrics.empty:
        pair_metrics = pair_metrics.sort_values(
            ["shared_transactions", "lift", "balanced_confidence"],
            ascending=[False, False, False],
        ).reset_index(drop=True)

    rule_rows: list[dict] = []
    for _, row in pair_metrics.iterrows():
        for antecedent, consequent, confidence, conviction, antecedent_support, consequent_support in [
            (row["article_a"], row["article_b"], row["confidence_a_b"], row["conviction_a_b"], row["support_a"], row["support_b"]),
            (row["article_b"], row["article_a"], row["confidence_b_a"], row["conviction_b_a"], row["support_b"], row["support_a"]),
        ]:
            if confidence < thresholds["min_confidence"] or row["lift"] < thresholds["min_lift"]:
                continue
            rule_rows.append(
                {
                    "antecedent": antecedent,
                    "consequent": consequent,
                    "support": row["joint_support"],
                    "antecedent_support": antecedent_support,
                    "consequent_support": consequent_support,
                    "confidence": confidence,
                    "lift": row["lift"],
                    "leverage": row["leverage"],
                    "conviction": conviction,
                    "shared_transactions": row["shared_transactions"],
                }
            )

    rule_metrics = pd.DataFrame(rule_rows)
    if not rule_metrics.empty:
        rule_metrics = rule_metrics.sort_values(["lift", "confidence", "shared_transactions"], ascending=[False, False, False]).head(config.model.max_rules_output)

    item_metrics = pd.DataFrame(
        {
            "article": item_tx_counts.index,
            "transaction_frequency": item_tx_counts.values,
            "support": item_tx_counts.values / max(total_transactions, 1),
            "total_quantity": item_qty_totals.reindex(item_tx_counts.index).values,
        }
    ).sort_values("transaction_frequency", ascending=False)

    return AssociationResult(
        pair_metrics=pair_metrics,
        rule_metrics=rule_metrics,
        item_metrics=item_metrics.reset_index(drop=True),
        thresholds=thresholds,
    )
