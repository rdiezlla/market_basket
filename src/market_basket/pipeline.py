from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from .associations import AssociationResult, compute_associations
from .cleaning import CleanResult, clean_movements
from .clustering import ClusterResult, build_clusters
from .config import AppConfig
from .eda import EDAResult, build_eda_outputs
from .io import project_columns, read_input_excel, validate_required_columns
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
    cleaning: CleanResult
    transactions: TransactionBuildResult
    eda: EDAResult
    associations: AssociationResult
    temporal: TemporalResult
    scoring: ScoringResult
    clusters: ClusterResult
    metadata: dict


def run_pipeline(config: AppConfig) -> PipelineArtifacts:
    logger = get_logger()
    logger.info("Cargando archivo Excel de entrada")
    raw_result = read_input_excel(config)
    validate_required_columns(raw_result.dataframe, config)
    projected = project_columns(raw_result.dataframe, config)

    logger.info("Ejecutando limpieza y validaciones")
    cleaning = clean_movements(projected, config)

    logger.info("Construyendo transacciones compuestas Pedido externo + Propietario")
    transactions = build_transactions(cleaning.clean_df)

    logger.info("Generando EDA")
    eda = build_eda_outputs(cleaning.clean_df, transactions.transactions_df, transactions.tx_item_df, cleaning.sku_attributes)

    logger.info("Calculando afinidad SKU-SKU")
    associations = compute_associations(transactions.tx_item_df, config)

    logger.info("Calculando estabilidad temporal")
    temporal = compute_temporal_stability(transactions.tx_item_df, config)

    logger.info("Aplicando score final de layout")
    scoring = compute_layout_scores(associations.pair_metrics, temporal.stability_metrics, config)

    logger.info("Detectando clusters y hubs")
    if config.model.min_pair_transactions is None:
        config.model.min_pair_transactions = int(associations.thresholds["min_pair_transactions"])
    clusters = build_clusters(scoring.scored_pairs, eda.article_summary, config)
    if not clusters.hub_summary.empty:
        eda.article_summary = eda.article_summary.merge(clusters.hub_summary, on="article", how="left")

    output_paths = prepare_output_paths(config)
    logger.info("Escribiendo salidas")

    quality_export = pd.concat([cleaning.quality_summary, cleaning.null_summary.assign(issue="null_summary")], ignore_index=True, sort=False)
    aggregate_time_series = eda.time_series.copy()
    aggregate_time_series["series_type"] = "aggregate_transactions"
    pair_time_series = temporal.temporal_pairs.copy()
    if not pair_time_series.empty:
        pair_time_series["series_type"] = "pair_affinity"
    series_temporales = pd.concat([aggregate_time_series, pair_time_series], ignore_index=True, sort=False)
    for column in ["granularity", "period", "series_type", "article_a", "article_b", "period_label"]:
        if column in series_temporales.columns:
            series_temporales[column] = series_temporales[column].astype("string")

    save_dataframe("kpi_resumen", eda.kpi_summary, output_paths, config)
    save_dataframe("calidad_datos", quality_export, output_paths, config)
    save_dataframe("transacciones_resumen", eda.transaction_summary, output_paths, config)
    save_dataframe("articulos_resumen", eda.article_summary, output_paths, config)
    save_dataframe("articulos_por_propietario", eda.owner_article_summary, output_paths, config)
    save_dataframe("afinidad_pares", scoring.scored_pairs, output_paths, config)
    save_dataframe("afinidad_reglas", associations.rule_metrics, output_paths, config)
    save_dataframe("clusters_sku", clusters.cluster_summary, output_paths, config)
    save_dataframe("hubs_sku", clusters.hub_summary, output_paths, config)
    save_dataframe("series_temporales", series_temporales, output_paths, config)
    save_quality_logs(cleaning.excluded_missing_order, cleaning.null_summary, output_paths)

    plots = create_visualizations(
        transactions.transactions_df,
        eda.article_summary,
        scoring.scored_pairs,
        temporal.temporal_pairs,
        clusters.graph_edges,
        output_paths,
        config,
    )
    render_executive_summary(
        output_paths.base_dir / "resumen_ejecutivo.md",
        cleaning.quality_summary,
        eda.kpi_summary,
        scoring.scored_pairs,
        clusters.cluster_summary,
        clusters.hub_summary,
    )

    metadata = {
        "execution_timestamp": datetime.now().isoformat(),
        "input_file": str(raw_result.input_path),
        "filters_applied": {
            "movement_type": config.model.valid_movement_type,
            "transaction_definition": "Pedido externo + Propietario",
            "excluded_missing_external_order_from_main_model": True,
        },
        "analyzed_date_range": {"min": cleaning.profile.get("min_date"), "max": cleaning.profile.get("max_date")},
        "dataset_profile": cleaning.profile,
        "transactions": int(transactions.transactions_df["transaction_id"].nunique()),
        "skus": int(transactions.tx_item_df["article"].nunique()),
        "owners": int(transactions.transactions_df["owner"].nunique()),
        "model_parameters": {
            **associations.thresholds,
            "score_weights": config.model.score_weights,
            "cluster_similarity_threshold": config.model.cluster_similarity_threshold,
            "cluster_min_size": config.model.cluster_min_size,
            "temporal_windows_days": config.temporal.rolling_windows_days,
        },
        "score_formula": scoring.score_metadata,
        "generated_plots": plots,
        "samples": {
            "top_pairs": dataframe_to_records(scoring.scored_pairs, limit=10),
            "top_clusters": dataframe_to_records(clusters.cluster_summary, limit=5),
        },
    }
    save_metadata(metadata, output_paths)

    return PipelineArtifacts(
        cleaning=cleaning,
        transactions=transactions,
        eda=eda,
        associations=associations,
        temporal=temporal,
        scoring=scoring,
        clusters=clusters,
        metadata=metadata,
    )
