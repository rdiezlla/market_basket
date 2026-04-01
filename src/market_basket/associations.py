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
    thresholds: dict[str, object]


def _count_pairs_from_transactions(
    tx_item_df: pd.DataFrame,
    use_sparse_pair_engine: bool = False,
) -> tuple[Counter[tuple[str, str]], Counter[tuple[str, str]]]:
    pair_counter: Counter[tuple[str, str]] = Counter()
    weighted_pair_counter: Counter[tuple[str, str]] = Counter()

    if use_sparse_pair_engine:
        # Reserved for a future sparse-matrix backend; current fallback keeps behavior explicit.
        use_sparse_pair_engine = False

    for _, group in tx_item_df.groupby("transaction_id"):
        article_qty = dict(zip(group["article"].astype(str), group["quantity_sum"].astype(float)))
        basket = sorted(article_qty)
        if len(basket) < 2:
            continue
        for article_a, article_b in combinations(basket, 2):
            pair_counter[(article_a, article_b)] += 1
            weighted_pair_counter[(article_a, article_b)] += min(article_qty[article_a], article_qty[article_b])

    return pair_counter, weighted_pair_counter


def derive_thresholds(
    transaction_count: int,
    pair_counter: Counter[tuple[str, str]],
    config: AppConfig,
) -> dict[str, object]:
    pair_cfg = config.thresholds.pairs
    rule_cfg = config.thresholds.rules
    clustering_cfg = config.thresholds.clustering

    pair_counts = pd.Series(pair_counter, dtype=float)
    if pair_cfg.min_pair_transactions is not None:
        min_pair_transactions = int(pair_cfg.min_pair_transactions)
        strategy = "configured_explicit_count"
    else:
        support_driven_count = int(np.ceil(transaction_count * pair_cfg.adaptive_support_floor))
        quantile_count = (
            int(
                max(
                    pair_cfg.adaptive_min_count,
                    min(pair_cfg.adaptive_max_count, pair_counts.quantile(pair_cfg.adaptive_pair_count_quantile)),
                )
            )
            if not pair_counts.empty
            else pair_cfg.adaptive_min_count
        )
        min_pair_transactions = max(pair_cfg.adaptive_min_count, support_driven_count, quantile_count)
        strategy = "adaptive_support_and_quantile"

    min_support = pair_cfg.min_support if pair_cfg.min_support is not None else safe_div(min_pair_transactions, transaction_count)
    min_edge_shared = (
        clustering_cfg.min_edge_shared_transactions
        if clustering_cfg.min_edge_shared_transactions is not None
        else min_pair_transactions
    )

    return {
        "pairs": {
            "min_pair_transactions": int(min_pair_transactions),
            "min_support": float(min_support),
            "raw_temporal_min_shared_transactions": int(pair_cfg.raw_temporal_min_shared_transactions),
            "strategy": strategy,
            "strategy_detail": {
                "adaptive_support_floor": pair_cfg.adaptive_support_floor,
                "adaptive_pair_count_quantile": pair_cfg.adaptive_pair_count_quantile,
                "adaptive_min_count": pair_cfg.adaptive_min_count,
                "adaptive_max_count": pair_cfg.adaptive_max_count,
                "transaction_count": transaction_count,
            },
        },
        "rules": {
            "min_confidence": float(rule_cfg.min_confidence),
            "min_lift": float(rule_cfg.min_lift),
            "max_rules_output": int(rule_cfg.max_rules_output),
            "exclude_frequent_articles_above_support": rule_cfg.exclude_frequent_articles_above_support,
        },
        "clustering": {
            "min_cluster_size": int(clustering_cfg.min_cluster_size),
            "similarity_threshold": float(clustering_cfg.similarity_threshold),
            "min_edge_shared_transactions": int(min_edge_shared),
        },
        "performance": {
            "pair_count_backend": "python_combinations",
            "use_sparse_pair_engine_requested": bool(config.performance.use_sparse_pair_engine),
        },
    }


def _compute_pair_metrics(
    tx_item_df: pd.DataFrame,
    item_tx_counts: pd.Series,
    item_qty_totals: pd.Series,
    pair_counter: Counter[tuple[str, str]],
    weighted_pair_counter: Counter[tuple[str, str]],
    total_transactions: int,
    min_pair_transactions: int,
) -> pd.DataFrame:
    pair_rows: list[dict] = []

    for (article_a, article_b), shared_transactions in pair_counter.items():
        if shared_transactions < min_pair_transactions:
            continue

        weighted_shared_qty = weighted_pair_counter[(article_a, article_b)]
        support_a = safe_div(item_tx_counts.get(article_a, 0), total_transactions)
        support_b = safe_div(item_tx_counts.get(article_b, 0), total_transactions)
        joint_support = safe_div(shared_transactions, total_transactions)
        expected_joint_support = support_a * support_b
        confidence_a_b = safe_div(shared_transactions, item_tx_counts.get(article_a, 0))
        confidence_b_a = safe_div(shared_transactions, item_tx_counts.get(article_b, 0))
        lift = safe_div(joint_support, expected_joint_support)
        leverage = joint_support - expected_joint_support
        conviction_a_b = np.inf if confidence_a_b >= 1 else safe_div(1 - support_b, 1 - confidence_a_b)
        conviction_b_a = np.inf if confidence_b_a >= 1 else safe_div(1 - support_a, 1 - confidence_b_a)
        jaccard = safe_div(shared_transactions, item_tx_counts.get(article_a, 0) + item_tx_counts.get(article_b, 0) - shared_transactions)
        cosine = safe_div(shared_transactions, np.sqrt(item_tx_counts.get(article_a, 0) * item_tx_counts.get(article_b, 0)))
        weighted_cosine = safe_div(weighted_shared_qty, np.sqrt(item_qty_totals.get(article_a, 0) * item_qty_totals.get(article_b, 0)))
        balanced_confidence = harmonic_mean([confidence_a_b, confidence_b_a])
        popularity_score = max(support_a, support_b)
        pmi = np.log(joint_support / expected_joint_support) if joint_support > 0 and expected_joint_support > 0 else 0.0
        npmi = pmi / -np.log(joint_support) if joint_support > 0 and joint_support != 1 else 0.0

        pair_rows.append(
            {
                "article_a": article_a,
                "article_b": article_b,
                "shared_transactions": int(shared_transactions),
                "joint_support": joint_support,
                "support_a": support_a,
                "support_b": support_b,
                "expected_joint_support": expected_joint_support,
                "confidence_a_b": confidence_a_b,
                "confidence_b_a": confidence_b_a,
                "balanced_confidence": balanced_confidence,
                "lift": lift,
                "leverage": leverage,
                "residual_cooccurrence": joint_support - expected_joint_support,
                "conviction_a_b": conviction_a_b,
                "conviction_b_a": conviction_b_a,
                "jaccard_similarity": jaccard,
                "cosine_similarity": cosine,
                "weighted_shared_quantity": weighted_shared_qty,
                "weighted_cosine_similarity": weighted_cosine,
                "pmi": pmi,
                "npmi": npmi,
                "popularity_penalty_factor": 1.0,
                "max_item_support": popularity_score,
            }
        )

    pair_metrics = pd.DataFrame(pair_rows)
    if not pair_metrics.empty:
        pair_metrics = pair_metrics.sort_values(
            ["shared_transactions", "joint_support", "lift"],
            ascending=[False, False, False],
        ).reset_index(drop=True)
    return pair_metrics


def _build_item_metrics(
    tx_item_df: pd.DataFrame,
    item_tx_counts: pd.Series,
    item_qty_totals: pd.Series,
    rule_support_cap: float | None,
    total_transactions: int,
) -> pd.DataFrame:
    item_metrics = (
        pd.DataFrame(
            {
                "article": item_tx_counts.index,
                "transaction_frequency": item_tx_counts.values,
                "support": item_tx_counts.values / max(total_transactions, 1),
                "row_frequency": tx_item_df.groupby("article")["transaction_id"].size().reindex(item_tx_counts.index).values,
                "total_quantity": item_qty_totals.reindex(item_tx_counts.index).values,
                "mean_quantity_per_tx": tx_item_df.groupby("article")["quantity_sum"].mean().reindex(item_tx_counts.index).values,
            }
        )
        .sort_values("transaction_frequency", ascending=False)
        .reset_index(drop=True)
    )
    item_metrics["excluded_from_rules_flag"] = (
        item_metrics["support"] > rule_support_cap
        if rule_support_cap is not None
        else False
    )
    return item_metrics


def _build_rule_metrics(
    pair_metrics: pd.DataFrame,
    thresholds: dict[str, object],
) -> pd.DataFrame:
    if pair_metrics.empty:
        return pd.DataFrame()

    rule_cfg = thresholds["rules"]
    support_cap = rule_cfg["exclude_frequent_articles_above_support"]
    rule_rows: list[dict] = []

    for row in pair_metrics.itertuples(index=False):
        for antecedent, consequent, confidence, conviction, antecedent_support, consequent_support in [
            (row.article_a, row.article_b, row.confidence_a_b, row.conviction_a_b, row.support_a, row.support_b),
            (row.article_b, row.article_a, row.confidence_b_a, row.conviction_b_a, row.support_b, row.support_a),
        ]:
            if confidence < rule_cfg["min_confidence"] or row.lift < rule_cfg["min_lift"]:
                continue
            if support_cap is not None and max(antecedent_support, consequent_support) > support_cap:
                continue
            rule_rows.append(
                {
                    "antecedent": antecedent,
                    "consequent": consequent,
                    "support": row.joint_support,
                    "antecedent_support": antecedent_support,
                    "consequent_support": consequent_support,
                    "confidence": confidence,
                    "lift": row.lift,
                    "leverage": row.leverage,
                    "conviction": conviction,
                    "npmi": row.npmi,
                    "shared_transactions": row.shared_transactions,
                }
            )

    rule_metrics = pd.DataFrame(rule_rows)
    if not rule_metrics.empty:
        rule_metrics = rule_metrics.sort_values(
            ["lift", "confidence", "shared_transactions"],
            ascending=[False, False, False],
        ).head(int(rule_cfg["max_rules_output"]))
    return rule_metrics


def compute_associations(
    tx_item_df: pd.DataFrame,
    config: AppConfig,
    *,
    min_pair_transactions_override: int | None = None,
) -> AssociationResult:
    if tx_item_df.empty:
        empty = pd.DataFrame()
        return AssociationResult(pair_metrics=empty, rule_metrics=empty, item_metrics=empty, thresholds={})

    total_transactions = int(tx_item_df["transaction_id"].nunique())
    item_tx_counts = tx_item_df.groupby("article")["transaction_id"].nunique().sort_values(ascending=False)
    item_qty_totals = tx_item_df.groupby("article")["quantity_sum"].sum()
    pair_counter, weighted_pair_counter = _count_pairs_from_transactions(
        tx_item_df,
        use_sparse_pair_engine=config.performance.use_sparse_pair_engine,
    )
    thresholds = derive_thresholds(total_transactions, pair_counter, config)
    pair_thresholds = thresholds["pairs"]
    applied_min_pair_transactions = (
        int(min_pair_transactions_override)
        if min_pair_transactions_override is not None
        else int(pair_thresholds["min_pair_transactions"])
    )

    pair_metrics = _compute_pair_metrics(
        tx_item_df=tx_item_df,
        item_tx_counts=item_tx_counts,
        item_qty_totals=item_qty_totals,
        pair_counter=pair_counter,
        weighted_pair_counter=weighted_pair_counter,
        total_transactions=total_transactions,
        min_pair_transactions=applied_min_pair_transactions,
    )
    if not pair_metrics.empty:
        pair_metrics["popularity_penalty_factor"] = 1.0 / (
            1.0 + config.model.popularity_penalty_alpha * pair_metrics["max_item_support"]
        )

    item_metrics = _build_item_metrics(
        tx_item_df=tx_item_df,
        item_tx_counts=item_tx_counts,
        item_qty_totals=item_qty_totals,
        rule_support_cap=thresholds["rules"]["exclude_frequent_articles_above_support"],
        total_transactions=total_transactions,
    )
    rule_metrics = _build_rule_metrics(pair_metrics, thresholds)
    thresholds["pairs"]["applied_min_pair_transactions"] = applied_min_pair_transactions

    return AssociationResult(
        pair_metrics=pair_metrics,
        rule_metrics=rule_metrics,
        item_metrics=item_metrics,
        thresholds=thresholds,
    )
