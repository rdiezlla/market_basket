from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .config import AppConfig
from .utils import canonicalize_identifier, clean_string, normalize_column_name


@dataclass
class RawLoadResult:
    dataframe: pd.DataFrame
    column_mapping: dict[str, str]
    input_path: Path


def read_input_excel(config: AppConfig) -> RawLoadResult:
    input_path = Path(config.paths.input_excel)
    dataframe = pd.read_excel(input_path)
    column_mapping = {normalize_column_name(column): str(column) for column in dataframe.columns}
    dataframe.columns = [normalize_column_name(column) for column in dataframe.columns]
    return RawLoadResult(dataframe=dataframe, column_mapping=column_mapping, input_path=input_path)


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
        raise ValueError(f"Faltan columnas requeridas en el Excel: {missing}")


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
    df = dataframe[list(columns.values())].copy()
    df.columns = list(columns.keys())

    for column in ["movement_type", "article_description", "location"]:
        df[column] = df[column].map(clean_string).astype("string")
    for column in ["article", "owner", "external_order"]:
        df[column] = df[column].map(canonicalize_identifier).astype("string")

    return df
