from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .config import AppConfig
from .utils import canonicalize_identifier, clean_string, get_logger, normalize_column_name


@dataclass
class RawLoadResult:
    dataframe: pd.DataFrame
    column_mapping: dict[str, list[str]]
    input_path: Path
    input_format: str


def _build_column_mapping(columns: list[object]) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    for column in columns:
        normalized = normalize_column_name(column)
        mapping.setdefault(normalized, []).append(str(column))
    duplicates = {key: value for key, value in mapping.items() if len(value) > 1}
    if duplicates:
        duplicate_repr = ", ".join(f"{key}: {value}" for key, value in duplicates.items())
        raise ValueError(
            "Se detectaron columnas duplicadas tras normalizacion. "
            f"Revisa el fichero de entrada. Duplicados: {duplicate_repr}"
        )
    return mapping


def _read_by_extension(path: Path, sheet_name: str | int | None) -> tuple[pd.DataFrame, str]:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        return pd.read_excel(path, sheet_name=sheet_name), "excel"
    if suffix == ".csv":
        return pd.read_csv(path), "csv"
    if suffix == ".parquet":
        return pd.read_parquet(path), "parquet"
    raise ValueError(
        f"Formato de entrada no soportado para {path.name}. "
        "Usa Excel, CSV o Parquet."
    )


def read_input_dataset(config: AppConfig) -> RawLoadResult:
    logger = get_logger()
    input_path = Path(config.paths.input_data)
    if not input_path.exists():
        raise FileNotFoundError(f"No se encontro el fichero de entrada: {input_path}")

    try:
        dataframe, input_format = _read_by_extension(input_path, config.paths.sheet_name)
    except ValueError:
        raise
    except Exception as exc:
        raise RuntimeError(
            f"No se pudo leer el fichero de entrada {input_path}. "
            "Verifica extension, permisos y consistencia del formato."
        ) from exc

    if isinstance(dataframe, dict):
        available = list(dataframe)
        raise ValueError(
            "La lectura devolvio varias hojas. Define paths.sheet_name en el YAML. "
            f"Hojas disponibles: {available}"
        )

    column_mapping = _build_column_mapping(list(dataframe.columns))
    dataframe.columns = [normalize_column_name(column) for column in dataframe.columns]
    logger.info(
        "Lectura completada | formato=%s | filas=%s | columnas=%s",
        input_format,
        len(dataframe),
        len(dataframe.columns),
    )
    logger.info("Mapping columnas originales-normalizadas: %s", column_mapping)

    return RawLoadResult(
        dataframe=dataframe,
        column_mapping=column_mapping,
        input_path=input_path,
        input_format=input_format,
    )


def read_input_excel(config: AppConfig) -> RawLoadResult:
    return read_input_dataset(config)


def validate_required_columns(dataframe: pd.DataFrame, config: AppConfig) -> None:
    expected = {
        normalize_column_name(config.columns.movement_type),
        normalize_column_name(config.columns.completion_date),
        normalize_column_name(config.columns.article),
        normalize_column_name(config.columns.article_description),
        normalize_column_name(config.columns.quantity),
        normalize_column_name(config.columns.owner),
        normalize_column_name(config.columns.location),
        normalize_column_name(config.columns.external_order),
    }
    missing = sorted(expected.difference(set(dataframe.columns)))
    if missing:
        raise ValueError(f"Faltan columnas requeridas en el fichero de entrada: {missing}")


def project_columns(dataframe: pd.DataFrame, config: AppConfig) -> pd.DataFrame:
    columns = {
        "movement_type": normalize_column_name(config.columns.movement_type),
        "completion_date": normalize_column_name(config.columns.completion_date),
        "article": normalize_column_name(config.columns.article),
        "article_description": normalize_column_name(config.columns.article_description),
        "quantity": normalize_column_name(config.columns.quantity),
        "owner": normalize_column_name(config.columns.owner),
        "location": normalize_column_name(config.columns.location),
        "external_order": normalize_column_name(config.columns.external_order),
    }

    projected = dataframe[list(columns.values())].copy()
    projected.columns = list(columns.keys())

    for column in ["movement_type", "article_description", "location"]:
        projected[column] = projected[column].map(clean_string).astype("string")
    for column in ["article", "owner", "external_order"]:
        projected[column] = projected[column].map(canonicalize_identifier).astype("string")

    return projected
