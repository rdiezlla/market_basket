from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from . import __version__
from .associations import AssociationResult, compute_associations
from .cleaning import CleanResult, clean_movements
from .clustering import ClusterResult, build_clusters
from .config import AppConfig
from .eda import EDAResult, build_eda_outputs
from .io import RawLoadResult, project_columns, read_input_dataset, validate_required_columns
from .outputs import (
    create_visualizations,
    prepare_output_paths,
    render_executive_summary,
    save_dataframe,
    save_metadata,
    save_quality_logs,
)
from .scoring import ScoringResult, compute_layout_scores
from .temporal import TemporalResult, compute_temporal_stability
from .transactions import TransactionBuildResult, build_transactions
from .utils import dataframe_to_records, get_logger


@dataclass
class PipelineArtifacts:
    raw: RawLoadResult
    cleaning: CleanResult
    transactions: TransactionBuildResult
    eda: EDAResult
    associations: AssociationResult
    temporal: TemporalResult
    scoring: ScoringResult
    clusters: ClusterResult
    metadata: dict


def _build_series_temporales(eda: EDAResult, temporal: TemporalResult) -> pd.DataFrame:
    aggregate_time_series = eda.time_series.copy()
    if not aggregate_time_series.empty:
        aggregate_time_series["series_type"] = "aggregate_transactions"

    pair_time_series = temporal.temporal_pairs.copy()
    if not pair_time_series.empty:
        pair_time_series["series_type"] = "pair_affinity"

    series_temporales = pd.concat([aggregate_time_series, pair_time_series], ignore_index=True, sort=False)
    for column in [
        "granularity",
        "period",
        "series_type",
        "article_a",
        "article_b",
        "period_label",
        "period_granularity",
        "support_trend",
        "lift_trend",
        "trend_classification",
    ]:
        if column in series_temporales.columns:
            series_temporales[column] = series_temporales[column].astype("string")
    return series_temporales


def run_pipeline(config: AppConfig) -> PipelineArtifacts:
    logger = get_logger()
    logger.info("Stage 1/8 | Reading input dataset")
    raw = read_input_dataset(config)
    validate_required_columns(raw.dataframe, config)
    projected = project_columns(raw.dataframe, config)
    if projected.empty:
        raise ValueError("No rows were loaded from the input dataset after projection.")

    logger.info("Stage 2/8 | Cleaning and data-quality rules")
    cleaning = clean_movements(projected, config)
    if cleaning.clean_df.empty:
        raise ValueError("The main model dataset is empty after cleaning and business filters.")

    logger.info("Stage 3/8 | Building transactions")
    transactions = build_transactions(cleaning.clean_df, config)
    if transactions.transactions_df.empty or transactions.tx_item_df.empty:
        raise ValueError("Transaction generation returned empty outputs.")

    logger.info("Stage 4/8 | Building EDA outputs")
    eda = build_eda_outputs(cleaning.clean_df, transactions.transactions_df, transactions.tx_item_df, cleaning.sku_attributes)

    logger.info("Stage 5/8 | Computing associations and rules")
    associations = compute_associations(transactions.tx_item_df, config)

    logger.info("Stage 6/8 | Computing raw temporal pairs and stability")
    temporal = compute_temporal_stability(transactions.tx_item_df, config)

    logger.info("Stage 7/8 | Computing layout scores and SKU graph")
    scoring = compute_layout_scores(associations.pair_metrics, temporal.stability_metrics, config)
    min_edge_shared_transactions = int(associations.thresholds.get("clustering", {}).get("min_edge_shared_transactions", 1))
    clusters = build_clusters(scoring.scored_pairs, eda.article_summary, config, min_edge_shared_transactions)
    if not clusters.hub_summary.empty and not eda.article_summary.empty:
        eda.article_summary = eda.article_summary.merge(clusters.hub_summary, on="article", how="left")

    logger.info("Stage 8/8 | Writing outputs, metadata and plots")
    output_paths = prepare_output_paths(config)
    quality_export = pd.concat(
        [
            cleaning.quality_summary,
            cleaning.null_summary.assign(issue="null_summary", severity="info", stage="raw", rule=pd.NA, description="Null profile by column."),
        ],
        ignore_index=True,
        sort=False,
    )
    series_temporales = _build_series_temporales(eda, temporal)

    save_dataframe("kpi_resumen", eda.kpi_summary, output_paths, config)
    save_dataframe("calidad_datos", quality_export, output_paths, config)
    save_dataframe("transacciones_resumen", eda.transaction_summary, output_paths, config)
    save_dataframe("articulos_resumen", eda.article_summary, output_paths, config)
    save_dataframe("articulos_por_propietario", eda.owner_article_summary, output_paths, config)
    save_dataframe("sku_location_profile", cleaning.sku_location_profile, output_paths, config)
    save_dataframe("item_metrics", associations.item_metrics, output_paths, config)
    save_dataframe("afinidad_pares", scoring.scored_pairs, output_paths, config)
    save_dataframe("afinidad_reglas", associations.rule_metrics, output_paths, config)
    save_dataframe("clusters_sku", clusters.cluster_summary, output_paths, config)
    save_dataframe("hubs_sku", clusters.hub_summary, output_paths, config)
    save_dataframe("raw_temporal_pairs", temporal.raw_temporal_pairs, output_paths, config)
    save_dataframe("temporal_stability_metrics", temporal.stability_metrics, output_paths, config)
    save_dataframe("series_temporales", series_temporales, output_paths, config)
    save_quality_logs(cleaning.excluded_missing_order, cleaning.null_summary, output_paths)

    plots = create_visualizations(
        transactions_df=transactions.transactions_df,
        article_summary=eda.article_summary,
        scored_pairs=scoring.scored_pairs,
        raw_temporal_pairs=temporal.raw_temporal_pairs,
        graph_edges=clusters.graph_edges,
        paths=output_paths,
        config=config,
    )
    render_executive_summary(
        summary_path=output_paths.base_dir / "resumen_ejecutivo.md",
        quality_summary=cleaning.quality_summary,
        kpi_summary=eda.kpi_summary,
        scored_pairs=scoring.scored_pairs,
        clusters_df=clusters.cluster_summary,
        hubs_df=clusters.hub_summary,
        stability_metrics=temporal.stability_metrics,
    )

    metadata = {
        "model_version": __version__,
        "execution_timestamp": datetime.now().isoformat(),
        "input_file": str(raw.input_path),
        "input_format": raw.input_format,
        "sheet_name": config.paths.sheet_name,
        "column_mapping": raw.column_mapping,
        "filters_applied": {
            "movement_type": config.model.valid_movement_type,
            "transaction_definition": f"external_order{config.transaction.id_separator}owner",
            "transaction_date_strategy": config.transaction.date_strategy,
        },
        "analyzed_date_range": {"min": cleaning.profile.get("min_date"), "max": cleaning.profile.get("max_date")},
        "dataset_profile": cleaning.profile,
        "exclusions_applied": cleaning.exclusions_applied,
        "active_cleaning_rules": {
            "data_quality": config.data_quality.__dict__,
        },
        "transactions": int(transactions.transactions_df["transaction_id"].nunique()),
        "skus": int(transactions.tx_item_df["article"].nunique()),
        "owners": int(transactions.transactions_df["owner"].nunique()),
        "thresholds_used": associations.thresholds,
        "score_metadata": scoring.score_metadata,
        "performance": config.performance.__dict__,
        "generated_plots": plots,
        "samples": {
            "top_pairs": dataframe_to_records(scoring.scored_pairs, limit=10),
            "top_clusters": dataframe_to_records(clusters.cluster_summary, limit=5),
        },
    }
    save_metadata(metadata, output_paths)

    return PipelineArtifacts(
        raw=raw,
        cleaning=cleaning,
        transactions=transactions,
        eda=eda,
        associations=associations,
        temporal=temporal,
        scoring=scoring,
        clusters=clusters,
        metadata=metadata,
    )
