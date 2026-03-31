from __future__ import annotations

import json
import logging
import math
import re
import unicodedata
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


LOGGER_NAME = "market_basket"


def get_logger(name: str = LOGGER_NAME) -> logging.Logger:
    return logging.getLogger(name)


def setup_logging(log_file: Path, level: str = "INFO") -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def normalize_column_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
    return " ".join(text.strip().lower().split())


def canonicalize_identifier(value: object) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "<na>"}:
        return None
    if re.fullmatch(r"-?\d+\.0+", text):
        return text.split(".")[0]
    return text


def clean_string(value: object) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "<na>"}:
        return None
    return re.sub(r"\s+", " ", text)


def stable_mode(series: pd.Series) -> str | None:
    cleaned = series.dropna().astype(str)
    if cleaned.empty:
        return None
    counts = cleaned.value_counts()
    return str(counts.index[0])


def safe_div(numerator: float, denominator: float) -> float:
    if denominator in (0, 0.0) or pd.isna(denominator):
        return 0.0
    return float(numerator) / float(denominator)


def harmonic_mean(values: Iterable[float]) -> float:
    values = [float(v) for v in values if v is not None and v > 0]
    if not values:
        return 0.0
    return len(values) / sum(1.0 / v for v in values)


def bounded_inverse_cv(values: pd.Series) -> float:
    clean = values.replace([np.inf, -np.inf], np.nan).dropna()
    if clean.empty:
        return 0.0
    mean = float(clean.mean())
    if mean == 0:
        return 0.0
    cv = float(clean.std(ddof=0)) / mean
    return 1.0 / (1.0 + cv)


def log_scale(series: pd.Series) -> pd.Series:
    if series.empty:
        return series.astype(float)
    max_value = float(series.max())
    if max_value <= 0:
        return pd.Series(np.zeros(len(series)), index=series.index, dtype=float)
    return np.log1p(series.astype(float)) / math.log1p(max_value)


def minmax_scale(series: pd.Series, cap_quantile: float = 0.95) -> pd.Series:
    if series.empty:
        return series.astype(float)
    cap = float(series.quantile(cap_quantile))
    cap = max(cap, float(series.max()), 1e-9) if cap <= 0 else cap
    clipped = series.clip(lower=0, upper=cap)
    return clipped / cap


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path: Path, payload: dict) -> None:
    ensure_directory(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def dataframe_to_records(df: pd.DataFrame, limit: int | None = None) -> list[dict]:
    if limit is not None:
        df = df.head(limit)
    return json.loads(df.to_json(orient="records", force_ascii=False, date_format="iso"))
