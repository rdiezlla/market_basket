from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class ColumnConfig:
    movement_type: str = "Tipo movimiento"
    completion_date: str = "Fecha finalización"
    article: str = "Artículo"
    article_description: str = "Denominación artículo"
    quantity: str = "Cantidad"
    owner: str = "Propietario"
    location: str = "Ubicacion"
    external_order: str = "Pedido externo"


@dataclass
class PathConfig:
    input_excel: str = "movimientos.xlsx"
    output_dir: str = "output"
    logs_dir: str = "output/logs"
    plots_dir: str = "output/plots"


@dataclass
class TemporalConfig:
    rolling_windows_days: list[int] = field(default_factory=lambda: [365, 180, 90])
    include_yearly: bool = True
    include_quarterly: bool = True


@dataclass
class ModelConfig:
    valid_movement_type: str = "PI"
    top_n_articles: int = 30
    min_pair_transactions: int | None = None
    min_support: float | None = None
    min_confidence: float = 0.05
    min_lift: float = 1.05
    max_rules_output: int = 20000
    cluster_min_size: int = 2
    cluster_similarity_threshold: float = 0.18
    max_edges_for_clustering: int = 4000
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


@dataclass
class OutputConfig:
    write_csv: bool = True
    write_parquet: bool = True
    write_excel: bool = False


@dataclass
class AppConfig:
    paths: PathConfig = field(default_factory=PathConfig)
    columns: ColumnConfig = field(default_factory=ColumnConfig)
    temporal: TemporalConfig = field(default_factory=TemporalConfig)
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


def _from_dict(data: dict[str, Any]) -> AppConfig:
    return AppConfig(
        paths=PathConfig(**data.get("paths", {})),
        columns=ColumnConfig(**data.get("columns", {})),
        temporal=TemporalConfig(**data.get("temporal", {})),
        model=ModelConfig(**data.get("model", {})),
        outputs=OutputConfig(**data.get("outputs", {})),
        log_level=data.get("log_level", "INFO"),
    )


def load_config(config_path: str | Path | None = None) -> AppConfig:
    default = AppConfig().to_dict()
    if not config_path:
        return AppConfig()
    path = Path(config_path)
    loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    merged = _deep_merge(default, loaded)
    return _from_dict(merged)
