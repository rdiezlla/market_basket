from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import yaml


VALID_TRANSACTION_DATE_STRATEGIES = {"max_completion_date", "min_completion_date", "mode_date"}


@dataclass
class ColumnConfig:
    movement_type: str = "Tipo movimiento"
    completion_date: str = "Fecha finalizacion"
    article: str = "Articulo"
    article_description: str = "Denominacion articulo"
    quantity: str = "Cantidad"
    owner: str = "Propietario"
    location: str = "Ubicacion"
    external_order: str = "Pedido externo"


@dataclass
class PathConfig:
    input_data: str = "movimientos.xlsx"
    sheet_name: str | int | None = None
    output_dir: str = "output"
    logs_dir: str = "output/logs"
    plots_dir: str = "output/plots"


@dataclass
class DataQualityConfig:
    exclude_non_positive_quantity: bool = True
    drop_exact_duplicates: bool = False
    duplicate_subset: list[str] = field(
        default_factory=lambda: [
            "movement_type",
            "completion_date",
            "article",
            "quantity",
            "owner",
            "location",
            "external_order",
        ]
    )


@dataclass
class TransactionConfig:
    id_separator: str = "|"
    date_strategy: str = "max_completion_date"


@dataclass
class TemporalConfig:
    rolling_windows_days: list[int] = field(default_factory=lambda: [365, 180, 90])
    include_yearly: bool = True
    include_quarterly: bool = True


@dataclass
class PairThresholdConfig:
    min_pair_transactions: int | None = None
    min_support: float | None = None
    adaptive_support_floor: float = 0.0002
    adaptive_pair_count_quantile: float = 0.95
    adaptive_min_count: int = 3
    adaptive_max_count: int = 5
    raw_temporal_min_shared_transactions: int = 1


@dataclass
class RuleThresholdConfig:
    min_confidence: float = 0.05
    min_lift: float = 1.05
    max_rules_output: int = 20000
    exclude_frequent_articles_above_support: float | None = None


@dataclass
class ClusteringThresholdConfig:
    min_cluster_size: int = 2
    similarity_threshold: float = 0.18
    min_edge_shared_transactions: int | None = None


@dataclass
class ScoringThresholdConfig:
    proximity_bins: list[float] = field(default_factory=lambda: [0.25, 0.45, 0.65])
    proximity_labels: list[str] = field(default_factory=lambda: ["low", "medium", "high", "very_high"])


@dataclass
class ThresholdConfig:
    pairs: PairThresholdConfig = field(default_factory=PairThresholdConfig)
    rules: RuleThresholdConfig = field(default_factory=RuleThresholdConfig)
    clustering: ClusteringThresholdConfig = field(default_factory=ClusteringThresholdConfig)
    scoring: ScoringThresholdConfig = field(default_factory=ScoringThresholdConfig)


@dataclass
class PerformanceConfig:
    max_edges_for_clustering: int = 4000
    graph_plot_max_edges: int = 100
    heatmap_top_n: int = 20
    raw_temporal_min_period_transactions: int = 2
    use_sparse_pair_engine: bool = False
    clustering_method: str = "greedy_modularity"


@dataclass
class ModelConfig:
    valid_movement_type: str = "PI"
    top_n_articles: int = 30
    score_weights: dict[str, float] = field(
        default_factory=lambda: {
            "joint_frequency": 0.30,
            "lift": 0.12,
            "balanced_confidence": 0.14,
            "similarity": 0.12,
            "temporal_stability": 0.20,
            "weighted_volume": 0.12,
        }
    )
    popularity_penalty_alpha: float = 2.0
    recurrence_penalty_floor: float = 0.25
    key_relationships_to_plot: int = 5
    heatmap_metric: str = "final_layout_score"
    score_weight_policy: str = "normalize"


@dataclass
class OutputConfig:
    write_csv: bool = True
    write_parquet: bool = True
    write_excel: bool = False


@dataclass
class AppConfig:
    paths: PathConfig = field(default_factory=PathConfig)
    columns: ColumnConfig = field(default_factory=ColumnConfig)
    data_quality: DataQualityConfig = field(default_factory=DataQualityConfig)
    transaction: TransactionConfig = field(default_factory=TransactionConfig)
    temporal: TemporalConfig = field(default_factory=TemporalConfig)
    thresholds: ThresholdConfig = field(default_factory=ThresholdConfig)
    performance: PerformanceConfig = field(default_factory=PerformanceConfig)
    model: ModelConfig = field(default_factory=ModelConfig)
    outputs: OutputConfig = field(default_factory=OutputConfig)
    log_level: str = "INFO"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _deep_merge(base: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _migrate_legacy_config(data: dict[str, Any]) -> dict[str, Any]:
    migrated = dict(data)
    paths = dict(migrated.get("paths", {}))
    if "input_excel" in paths and "input_data" not in paths:
        paths["input_data"] = paths.pop("input_excel")
    migrated["paths"] = paths

    model = dict(migrated.get("model", {}))
    thresholds = dict(migrated.get("thresholds", {}))
    pair_thresholds = dict(thresholds.get("pairs", {}))
    rule_thresholds = dict(thresholds.get("rules", {}))
    clustering_thresholds = dict(thresholds.get("clustering", {}))

    if "min_pair_transactions" in model and "min_pair_transactions" not in pair_thresholds:
        pair_thresholds["min_pair_transactions"] = model.pop("min_pair_transactions")
    if "min_support" in model and "min_support" not in pair_thresholds:
        pair_thresholds["min_support"] = model.pop("min_support")
    if "min_confidence" in model and "min_confidence" not in rule_thresholds:
        rule_thresholds["min_confidence"] = model.pop("min_confidence")
    if "min_lift" in model and "min_lift" not in rule_thresholds:
        rule_thresholds["min_lift"] = model.pop("min_lift")
    if "max_rules_output" in model and "max_rules_output" not in rule_thresholds:
        rule_thresholds["max_rules_output"] = model.pop("max_rules_output")
    if "cluster_min_size" in model and "min_cluster_size" not in clustering_thresholds:
        clustering_thresholds["min_cluster_size"] = model.pop("cluster_min_size")
    if "cluster_similarity_threshold" in model and "similarity_threshold" not in clustering_thresholds:
        clustering_thresholds["similarity_threshold"] = model.pop("cluster_similarity_threshold")

    performance = dict(migrated.get("performance", {}))
    if "max_edges_for_clustering" in model and "max_edges_for_clustering" not in performance:
        performance["max_edges_for_clustering"] = model.pop("max_edges_for_clustering")

    thresholds["pairs"] = pair_thresholds
    thresholds["rules"] = rule_thresholds
    thresholds["clustering"] = clustering_thresholds
    migrated["thresholds"] = thresholds
    migrated["performance"] = performance
    migrated["model"] = model
    return migrated


def _normalize_score_weights(config: AppConfig) -> None:
    weights = config.model.score_weights
    expected = {
        "joint_frequency",
        "lift",
        "balanced_confidence",
        "similarity",
        "temporal_stability",
        "weighted_volume",
    }
    missing = expected.difference(weights)
    extra = set(weights).difference(expected)
    if missing or extra:
        raise ValueError(
            "score_weights debe contener exactamente las claves esperadas. "
            f"Faltan: {sorted(missing)}. Sobran: {sorted(extra)}."
        )

    total = float(sum(weights.values()))
    if total <= 0:
        raise ValueError("La suma de score_weights debe ser mayor que 0.")

    if abs(total - 1.0) <= 1e-9:
        return

    if config.model.score_weight_policy == "normalize":
        config.model.score_weights = {key: float(value) / total for key, value in weights.items()}
        return

    raise ValueError(
        f"La suma de score_weights es {total:.6f} y no es 1. "
        "Ajusta los pesos o usa model.score_weight_policy=normalize."
    )


def validate_config(config: AppConfig) -> AppConfig:
    if not config.paths.input_data:
        raise ValueError("paths.input_data no puede estar vacio.")
    if not Path(config.paths.input_data).exists():
        raise FileNotFoundError(f"No se encontro el fichero de entrada: {config.paths.input_data}")

    if not config.paths.output_dir or not config.paths.logs_dir or not config.paths.plots_dir:
        raise ValueError("Las rutas de salida no pueden estar vacias.")

    if config.transaction.date_strategy not in VALID_TRANSACTION_DATE_STRATEGIES:
        raise ValueError(
            "transaction.date_strategy debe ser uno de "
            f"{sorted(VALID_TRANSACTION_DATE_STRATEGIES)}."
        )
    if not config.transaction.id_separator:
        raise ValueError("transaction.id_separator no puede estar vacio.")

    if config.data_quality.drop_exact_duplicates and not config.data_quality.duplicate_subset:
        raise ValueError("data_quality.duplicate_subset debe informar al menos una columna si se eliminan duplicados.")

    if any(days <= 0 for days in config.temporal.rolling_windows_days):
        raise ValueError("temporal.rolling_windows_days debe contener solo enteros positivos.")

    pair_cfg = config.thresholds.pairs
    rule_cfg = config.thresholds.rules
    clustering_cfg = config.thresholds.clustering
    scoring_cfg = config.thresholds.scoring

    if pair_cfg.min_pair_transactions is not None and pair_cfg.min_pair_transactions < 1:
        raise ValueError("thresholds.pairs.min_pair_transactions debe ser >= 1.")
    if pair_cfg.min_support is not None and not 0 < pair_cfg.min_support <= 1:
        raise ValueError("thresholds.pairs.min_support debe estar en el rango (0, 1].")
    if not 0 < pair_cfg.adaptive_support_floor <= 1:
        raise ValueError("thresholds.pairs.adaptive_support_floor debe estar en el rango (0, 1].")
    if not 0 < pair_cfg.adaptive_pair_count_quantile < 1:
        raise ValueError("thresholds.pairs.adaptive_pair_count_quantile debe estar en el rango (0, 1).")

    if not 0 <= rule_cfg.min_confidence <= 1:
        raise ValueError("thresholds.rules.min_confidence debe estar en el rango [0, 1].")
    if rule_cfg.min_lift < 0:
        raise ValueError("thresholds.rules.min_lift no puede ser negativo.")
    if rule_cfg.exclude_frequent_articles_above_support is not None and not 0 < rule_cfg.exclude_frequent_articles_above_support <= 1:
        raise ValueError("thresholds.rules.exclude_frequent_articles_above_support debe estar en el rango (0, 1].")

    if clustering_cfg.min_cluster_size < 2:
        raise ValueError("thresholds.clustering.min_cluster_size debe ser >= 2.")
    if not 0 <= clustering_cfg.similarity_threshold <= 1:
        raise ValueError("thresholds.clustering.similarity_threshold debe estar en el rango [0, 1].")
    if clustering_cfg.min_edge_shared_transactions is not None and clustering_cfg.min_edge_shared_transactions < 1:
        raise ValueError("thresholds.clustering.min_edge_shared_transactions debe ser >= 1.")

    bins = list(scoring_cfg.proximity_bins)
    if bins != sorted(bins) or any(not 0 < value < 1 for value in bins):
        raise ValueError("thresholds.scoring.proximity_bins debe ser una lista ordenada con valores en el rango (0, 1).")
    if len(scoring_cfg.proximity_labels) != len(bins) + 1:
        raise ValueError("thresholds.scoring.proximity_labels debe tener exactamente un elemento mas que proximity_bins.")

    if config.performance.max_edges_for_clustering < 1 or config.performance.graph_plot_max_edges < 1:
        raise ValueError("Los limites de performance para clustering y plots deben ser >= 1.")
    if config.performance.raw_temporal_min_period_transactions < 1:
        raise ValueError("performance.raw_temporal_min_period_transactions debe ser >= 1.")

    if not 0 < config.model.recurrence_penalty_floor <= 1:
        raise ValueError("model.recurrence_penalty_floor debe estar en el rango (0, 1].")
    if config.model.popularity_penalty_alpha < 0:
        raise ValueError("model.popularity_penalty_alpha no puede ser negativo.")

    _normalize_score_weights(config)
    return config


def _from_dict(data: dict[str, Any]) -> AppConfig:
    thresholds = data.get("thresholds", {})
    return AppConfig(
        paths=PathConfig(**data.get("paths", {})),
        columns=ColumnConfig(**data.get("columns", {})),
        data_quality=DataQualityConfig(**data.get("data_quality", {})),
        transaction=TransactionConfig(**data.get("transaction", {})),
        temporal=TemporalConfig(**data.get("temporal", {})),
        thresholds=ThresholdConfig(
            pairs=PairThresholdConfig(**thresholds.get("pairs", {})),
            rules=RuleThresholdConfig(**thresholds.get("rules", {})),
            clustering=ClusteringThresholdConfig(**thresholds.get("clustering", {})),
            scoring=ScoringThresholdConfig(**thresholds.get("scoring", {})),
        ),
        performance=PerformanceConfig(**data.get("performance", {})),
        model=ModelConfig(**data.get("model", {})),
        outputs=OutputConfig(**data.get("outputs", {})),
        log_level=data.get("log_level", "INFO"),
    )


def load_config(config_path: str | Path | None = None) -> AppConfig:
    default = AppConfig().to_dict()
    if not config_path:
        return validate_config(AppConfig())

    path = Path(config_path)
    loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    loaded = _migrate_legacy_config(loaded)
    merged = _deep_merge(default, loaded)
    return validate_config(_from_dict(merged))
