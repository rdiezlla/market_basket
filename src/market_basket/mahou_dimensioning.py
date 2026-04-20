from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import math
import re
import unicodedata
import xml.etree.ElementTree as ET
import zipfile

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ORIGIN_AISLES = [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 20]
DESTINATION_AISLES = list(range(1, 13))
DESTINATION_THEORETICAL_CAPACITY = 540.0
DESTINATION_HEIGHT_CAPACITY = 108.0
SCENARIO_RECOMMENDED = 0.90
SCENARIO_STRESS = 0.95

WIDTH_TO_EU_EQ = {
    "EU": 1.0,
    "AM": 1.5,
    "TR": 3.0,
    "S": 1.0,
    "00": 1.0,
    "BD": 1.0 / 9.0,
}

TIPOLOGY_TO_UNIT = {
    "EU": "posicion_rack",
    "AM": "posicion_rack",
    "TR": "posicion_rack",
    "suelo_estandar": "posicion_suelo",
    "suelo_250": "posicion_suelo",
    "suelo_300": "posicion_suelo",
    "suelo_126": "posicion_suelo",
    "balda_9h": "subhueco_balda",
}

TIPOLOGY_TO_DEFAULT_EU_EQ = {
    "EU": 1.0,
    "AM": 1.5,
    "TR": 3.0,
    "suelo_estandar": 1.0,
    "suelo_250": 1.0,
    "suelo_300": 1.0,
    "suelo_126": 1.0,
    "balda_9h": 1.0 / 9.0,
}

TIPOLOGY_TO_MODULE_DIVISOR = {
    "EU": 3.0,
    "AM": 2.0,
    "TR": 1.0,
    "suelo_estandar": 3.0,
    "suelo_250": 3.0,
    "suelo_300": 3.0,
    "suelo_126": 3.0,
    "balda_9h": 27.0,
}

PENALIZES_HEIGHT_10 = {"suelo_250", "suelo_300"}

OWNER_NAME_OVERRIDES = {
    3: "ALHAMBRA",
    4: "SOLAN",
    5: "SAN MIGUEL",
}


@dataclass
class PipelinePaths:
    base_dir: Path
    output_dir: Path
    csv_dir: Path
    plot_dir: Path
    support_dir: Path


def _normalize_text(value: object) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = str(value).strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    return text


def _normalize_column_name(value: object) -> str:
    text = _normalize_text(value)
    text = text.replace(".", " ").replace("/", " ")
    text = re.sub(r"[^A-Za-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_").lower()


def _canonical_identifier(value: object) -> str:
    text = _normalize_text(value).upper()
    if text.endswith(".0"):
        text = text[:-2]
    text = text.replace(" ", "")
    if not text:
        return ""
    if re.fullmatch(r"[0-9]+", text):
        return str(int(text))
    return text


def _safe_numeric(value: object) -> float | None:
    try:
        result = pd.to_numeric(value)
    except Exception:
        return None
    if pd.isna(result):
        return None
    return float(result)


def _normalize_dataframe_columns(frame: pd.DataFrame) -> pd.DataFrame:
    export = frame.copy()
    export.columns = [_normalize_column_name(column) for column in export.columns]
    return export


def _ensure_directories(base_dir: Path) -> PipelinePaths:
    output_dir = base_dir / "output" / "mahou_codex"
    csv_dir = output_dir / "csv"
    plot_dir = output_dir / "plots"
    support_dir = output_dir / "support"
    for path in [output_dir, csv_dir, plot_dir, support_dir]:
        path.mkdir(parents=True, exist_ok=True)
    return PipelinePaths(
        base_dir=base_dir,
        output_dir=output_dir,
        csv_dir=csv_dir,
        plot_dir=plot_dir,
        support_dir=support_dir,
    )


def _in_ranges(value: int, ranges: list[tuple[int, int]]) -> bool:
    return any(start <= value <= end for start, end in ranges)


def classify_tipology(pasillo: int, columna: int, altura: int, width: str) -> str:
    odd = columna % 2 == 1
    width = _normalize_text(width).upper()

    if width == "BD" or (
        pasillo == 10 and odd and 61 <= columna <= 125 and 1 <= altura <= 9
    ) or (
        pasillo == 11 and not odd and 44 <= columna <= 60 and 1 <= altura <= 9
    ) or (
        pasillo == 12 and odd and 43 <= columna <= 59 and 1 <= altura <= 9
    ):
        return "balda_9h"

    if altura == 0 and (
        (pasillo == 7 and 1 <= columna <= 70)
        or (pasillo == 8 and 1 <= columna <= 84)
        or (pasillo == 9 and 1 <= columna <= 84 and columna not in {67, 69, 76})
        or (pasillo == 10 and not odd and 62 <= columna <= 126)
        or (pasillo == 11 and odd and 61 <= columna <= 125)
        or (pasillo == 13 and not odd and _in_ranges(columna, [(2, 6), (62, 72)]))
        or (
            pasillo == 14
            and (
                (odd and 1 <= columna <= 11)
                or (not odd and 2 <= columna <= 12)
                or _in_ranges(columna, [(61, 72), (134, 162)])
            )
        )
        or (
            pasillo == 15
            and (
                (odd and _in_ranges(columna, [(1, 11), (61, 71), (133, 161)]))
                or (not odd and _in_ranges(columna, [(50, 78), (134, 162)]))
            )
        )
        or (pasillo == 16 and odd and 49 <= columna <= 77)
        or (pasillo == 20 and not odd)
    ):
        return "suelo_250"

    if altura == 0 and (
        (pasillo == 12 and not odd and _in_ranges(columna, [(2, 18), (62, 78)]))
        or (pasillo == 13 and odd and _in_ranges(columna, [(1, 17), (61, 77)]))
        or (pasillo == 18 and not odd)
        or pasillo == 19
        or (pasillo == 20 and odd)
    ):
        return "suelo_300"

    if altura == 0 and (
        (pasillo == 11 and not odd and 62 <= columna <= 126)
        or (
            pasillo == 12
            and (
                (odd and _in_ranges(columna, [(1, 41), (61, 125)]))
                or (not odd and 20 <= columna <= 60)
            )
        )
        or (
            pasillo == 13
            and (
                (odd and 19 <= columna <= 59)
                or (not odd and 14 <= columna <= 58)
            )
        )
        or (pasillo == 14 and 13 <= columna <= 60)
        or (
            pasillo == 15
            and (
                (odd and 13 <= columna <= 59)
                or (not odd and 2 <= columna <= 48)
            )
        )
        or (pasillo == 16 and odd and 1 <= columna <= 47)
    ):
        return "suelo_126"

    if altura == 0 and width in {"S", "00"}:
        return "suelo_estandar"

    if width in {"EU", "AM", "TR"}:
        return width

    if altura == 0:
        return "suelo_estandar"

    return "otros"


def _tipology_unit(tipology: str) -> str:
    return TIPOLOGY_TO_UNIT.get(tipology, "unidad_desconocida")


def _width_to_eu_eq(width: str, fallback_tipology: str | None = None) -> float:
    width = _normalize_text(width).upper()
    if width in WIDTH_TO_EU_EQ:
        return WIDTH_TO_EU_EQ[width]
    if fallback_tipology is not None:
        return TIPOLOGY_TO_DEFAULT_EU_EQ.get(fallback_tipology, 1.0)
    return 1.0


def _tipology_to_eu_eq(tipology: str, width_mode: str | None = None) -> float:
    if width_mode:
        return _width_to_eu_eq(width_mode, tipology)
    return TIPOLOGY_TO_DEFAULT_EU_EQ.get(tipology, 1.0)


def _tipology_to_modules(value_raw: float, tipology: str, width_mode: str | None = None) -> float:
    if tipology in {"EU", "AM", "TR"} and width_mode:
        eu_eq = value_raw * _width_to_eu_eq(width_mode, tipology)
        return eu_eq / 3.0
    divisor = TIPOLOGY_TO_MODULE_DIVISOR.get(tipology, 3.0)
    return value_raw / divisor


def _owner_prefix_from_reserva(text: str) -> int | None:
    match = re.match(r"^\s*(\d+)", _normalize_text(text))
    return int(match.group(1)) if match else None


def _owner_name_map(
    current_slots: pd.DataFrame,
    owner_map: pd.DataFrame,
) -> dict[int, str]:
    names: dict[int, str] = {}
    if "denominacion_propietario" in current_slots.columns:
        current_names = (
            current_slots.loc[current_slots["owner"].notna()]
            .groupby("owner")["denominacion_propietario"]
            .agg(lambda series: series.mode().iat[0] if not series.mode().empty else series.iloc[0])
        )
        for owner, name in current_names.items():
            if isinstance(owner, (int, float)) and not pd.isna(owner):
                names[int(owner)] = _normalize_text(name)

    if {"propietario_num", "departamento"}.issubset(owner_map.columns):
        owner_rows = owner_map.dropna(subset=["propietario_num"]).copy()
        owner_rows["propietario_num"] = owner_rows["propietario_num"].astype(int)
        for row in owner_rows.itertuples(index=False):
            names.setdefault(int(row.propietario_num), _normalize_text(row.departamento))

    for owner, name in OWNER_NAME_OVERRIDES.items():
        names.setdefault(owner, name)
    return names


DOCX_NS = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}


def _read_docx_tables(document_path: Path) -> list[list[list[str]]]:
    with zipfile.ZipFile(document_path) as archive:
        xml_bytes = archive.read("word/document.xml")
    root = ET.fromstring(xml_bytes)
    body = root.find("w:body", DOCX_NS)
    if body is None:
        return []

    tables: list[list[list[str]]] = []
    for table in body.findall("w:tbl", DOCX_NS):
        parsed_rows: list[list[str]] = []
        for row in table.findall("w:tr", DOCX_NS):
            parsed_cells = []
            for cell in row.findall("w:tc", DOCX_NS):
                texts = [
                    _normalize_text(node.text)
                    for node in cell.findall(".//w:t", DOCX_NS)
                    if _normalize_text(node.text)
                ]
                parsed_cells.append(" ".join(texts))
            if parsed_cells:
                parsed_rows.append(parsed_cells)
        if parsed_rows:
            tables.append(parsed_rows)
    return tables


def _extract_study_values(base_dir: Path) -> pd.DataFrame:
    previous_doc = Path(r"C:\Users\rdiezl\Downloads\Dimensionamiento operativo del nuevo almacén para la operativa de Mahou.docx")
    advanced_doc = next(base_dir.glob("Dimensionamiento operativo del nuevo almac*n Mahou.docx"))

    previous_tables = _read_docx_tables(previous_doc)
    advanced_tables = _read_docx_tables(advanced_doc)

    records: list[dict[str, object]] = []

    if len(previous_tables) >= 2:
        for row in previous_tables[1][1:]:
            if len(row) < 4:
                continue
            tipology = row[0].lower()
            records.append(
                {
                    "tipologia": f"{tipology}_valor_raw",
                    "study_previo_valor": row[1],
                    "study_previo_unidad": "valor_raw_documento",
                    "investigacion_avanzada_valor": pd.NA,
                    "investigacion_avanzada_unidad": pd.NA,
                }
            )
            records.append(
                {
                    "tipologia": f"{tipology}_modulos_3eu",
                    "study_previo_valor": row[3],
                    "study_previo_unidad": "modulo_3eu",
                    "investigacion_avanzada_valor": pd.NA,
                    "investigacion_avanzada_unidad": pd.NA,
                }
            )

    if len(advanced_tables) >= 3:
        for row in advanced_tables[1][1:]:
            if len(row) < 3:
                continue
            tipology = row[0].lower()
            records.append(
                {
                    "tipologia": f"{tipology}_tabla_valor_raw",
                    "study_previo_valor": pd.NA,
                    "study_previo_unidad": pd.NA,
                    "investigacion_avanzada_valor": row[1],
                    "investigacion_avanzada_unidad": "valor_raw_documento",
                }
            )
            records.append(
                {
                    "tipologia": f"{tipology}_tabla_modulos_3eu",
                    "study_previo_valor": pd.NA,
                    "study_previo_unidad": pd.NA,
                    "investigacion_avanzada_valor": row[2],
                    "investigacion_avanzada_unidad": "modulo_3eu",
                }
            )

    paragraph_records = [
        ("suelo_250_parrafo_modulos_3eu", "121"),
        ("suelo_300_parrafo_modulos_3eu", "40"),
        ("suelo_126_parrafo_modulos_3eu", "40"),
        ("balda_9h_parrafo_modulos_equivalentes", "557"),
    ]
    if advanced_doc.exists():
        for tipology, value in paragraph_records:
            records.append(
                {
                    "tipologia": tipology,
                    "study_previo_valor": pd.NA,
                    "study_previo_unidad": pd.NA,
                    "investigacion_avanzada_valor": value,
                    "investigacion_avanzada_unidad": "modulo_3eu" if "balda" not in tipology else "modulo_equivalente",
                }
            )

    return pd.DataFrame.from_records(records)


def _load_sources(base_dir: Path) -> dict[str, pd.DataFrame]:
    stock = _normalize_dataframe_columns(pd.read_excel(base_dir / "17-04-2026.xlsx"))
    dimensions = _normalize_dataframe_columns(pd.read_excel(base_dir / "maestro_dimensiones_limpio.xlsx"))
    movements = _normalize_dataframe_columns(pd.read_excel(base_dir / "movimientos.xlsx"))
    requests = _normalize_dataframe_columns(pd.read_excel(base_dir / "lineas_solicitudes_con_pedidos.xlsx"))
    external = _normalize_dataframe_columns(pd.read_excel(base_dir / "STOCK_MAP_FACT_MAHOU_260415020005.xlsx", sheet_name="STOCKS"))
    owner_map = _normalize_dataframe_columns(pd.read_excel(base_dir / "propietario_departamento.xlsx"))
    return {
        "stock": stock,
        "dimensions": dimensions,
        "movements": movements,
        "requests": requests,
        "external": external,
        "owner_map": owner_map,
    }


def _prepare_stock(stock: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    frame = stock.copy()
    frame["owner"] = pd.to_numeric(frame["propie"], errors="coerce")
    frame["pasillo"] = pd.to_numeric(frame["pasillo"], errors="coerce")
    frame["columna"] = pd.to_numeric(frame["col"], errors="coerce")
    frame["altura"] = pd.to_numeric(frame["alt"], errors="coerce")
    frame["stock_pal"] = pd.to_numeric(frame["stock_pal"], errors="coerce")
    frame["ubicacion"] = frame["ubicacion"].map(_normalize_text)
    frame["denominacion_propietario"] = frame.get("denominacion_propietario", "").map(_normalize_text)
    frame["article"] = frame["art_y"].map(_canonical_identifier)
    frame["denominacion"] = frame["denominacion"].map(_normalize_text)
    frame["width"] = frame["t_anc_pal"].map(lambda value: _normalize_text(value).upper())
    frame["occupied_flag"] = frame["stock_pal"].fillna(0).gt(0)

    filtered = frame[
        frame["owner"].le(100).fillna(False)
        & frame["occupied_flag"]
        & ~frame["ubicacion"].str.startswith("200-")
        & frame["pasillo"].isin(ORIGIN_AISLES)
    ].copy()

    filtered["tipologia"] = [
        classify_tipology(int(pasillo), int(columna), int(altura), width)
        for pasillo, columna, altura, width in zip(
            filtered["pasillo"], filtered["columna"], filtered["altura"], filtered["width"]
        )
    ]
    filtered["unit_resultante"] = filtered["tipologia"].map(_tipology_unit)
    filtered["eu_equivalente_factor"] = [
        _tipology_to_eu_eq(tipology, width)
        for tipology, width in zip(filtered["tipologia"], filtered["width"])
    ]

    slot_article = (
        filtered.groupby(["ubicacion", "owner", "article", "tipologia", "width"], dropna=False)
        .agg(
            stock_pal_total=("stock_pal", "sum"),
            pallet_rows=("codigo", "count"),
            denominacion=("denominacion", lambda series: series.mode().iat[0] if not series.mode().empty else series.iloc[0]),
            pasillo=("pasillo", "first"),
            columna=("columna", "first"),
            altura=("altura", "first"),
            denominacion_propietario=("denominacion_propietario", "first"),
        )
        .reset_index()
    )
    slot_article["owner"] = slot_article["owner"].astype(int)

    slot_level = (
        filtered.groupby("ubicacion", dropna=False)
        .agg(
            owners=("owner", lambda series: sorted({int(value) for value in series.dropna().tolist()})),
            owner_count=("owner", lambda series: series.dropna().nunique()),
            articles=("article", lambda series: sorted({value for value in series if value})),
            article_count=("article", lambda series: pd.Series([value for value in series if value]).nunique()),
            tipologia=("tipologia", lambda series: series.mode().iat[0] if not series.mode().empty else series.iloc[0]),
            width_mode=("width", lambda series: series.mode().iat[0] if not series.mode().empty else series.iloc[0]),
            stock_pal_total=("stock_pal", "sum"),
            pasillo=("pasillo", "first"),
            columna=("columna", "first"),
            altura=("altura", "first"),
            denominacion_propietario=("denominacion_propietario", lambda series: series.mode().iat[0] if not series.mode().empty else series.iloc[0]),
        )
        .reset_index()
    )
    slot_level["assigned_owner"] = np.where(
        slot_level["owner_count"] == 1,
        slot_level["owners"].map(lambda values: int(values[0])),
        pd.NA,
    )
    slot_level["owner_bucket"] = slot_level["assigned_owner"].fillna("sin_asignacion_segura")
    slot_level["unit_resultante"] = slot_level["tipologia"].map(_tipology_unit)
    slot_level["eu_equivalente_factor"] = [
        _tipology_to_eu_eq(tipology, width_mode)
        for tipology, width_mode in zip(slot_level["tipologia"], slot_level["width_mode"])
    ]

    conflicts = slot_level[(slot_level["owner_count"] > 1) | (slot_level["article_count"] > 1)].copy()
    return filtered, slot_article, slot_level, conflicts


def _prepare_movements(movements: pd.DataFrame) -> pd.DataFrame:
    frame = movements.copy()
    frame["tipo_movimiento"] = frame["tipo_movimiento"].map(_normalize_text).str.upper()
    frame["owner"] = pd.to_numeric(frame["propietario"], errors="coerce")
    frame["pasillo_origen"] = pd.to_numeric(frame["pas_ori"], errors="coerce")
    frame["cantidad"] = pd.to_numeric(frame["cantidad"], errors="coerce")
    frame["pedido_externo"] = frame["pedido_externo"].map(_normalize_text)
    frame["article"] = frame["articulo"].map(_canonical_identifier)
    frame["fecha_finalizacion"] = pd.to_datetime(frame["fecha_finalizacion"], errors="coerce", dayfirst=True)

    filtered = frame[
        frame["tipo_movimiento"].eq("PI")
        & frame["owner"].le(100).fillna(False)
        & frame["pasillo_origen"].isin(ORIGIN_AISLES)
        & frame["cantidad"].gt(0).fillna(False)
        & frame["pedido_externo"].ne("")
    ].copy()
    filtered["transaction_id"] = filtered["pedido_externo"] + "|" + filtered["owner"].astype(int).astype(str)
    return filtered


def _prepare_requests(requests: pd.DataFrame) -> pd.DataFrame:
    frame = requests.copy()
    frame["owner"] = pd.to_numeric(frame["propietario"], errors="coerce")
    frame["cantidad_solicitada"] = pd.to_numeric(frame["cant_solicitada"], errors="coerce")
    frame["pedido"] = frame["pedido"].map(_normalize_text)
    frame["fecha_servicio"] = pd.to_datetime(frame["fecha_de_servicio"], errors="coerce", dayfirst=True)
    return frame[frame["owner"].le(100).fillna(False)].copy()


def _prepare_dimensions(dimensions: pd.DataFrame) -> pd.DataFrame:
    frame = dimensions.copy()
    frame["article"] = frame["codigo"].map(_canonical_identifier)
    for column in ["largo", "ancho", "alto", "kilos", "m2", "m3"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["max_dimension_cm"] = frame[["largo", "ancho", "alto"]].max(axis=1)
    frame["categoria"] = frame["categoria"].map(_normalize_text)
    frame["nombre"] = frame["nombre"].map(_normalize_text)
    return frame


def _prepare_owner_map(owner_map: pd.DataFrame) -> pd.DataFrame:
    frame = owner_map.copy()
    frame["departamento"] = frame["departamento"].map(_normalize_text)
    frame["departamento_key"] = frame["departamento"].str.upper()
    frame["propietario_num"] = pd.to_numeric(frame["propietario"], errors="coerce")
    return frame


def _current_article_support(slot_article: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    slot_article = slot_article.copy()
    slot_article["eu_equivalente_factor"] = [
        _tipology_to_eu_eq(tipology, width)
        for tipology, width in zip(slot_article["tipologia"], slot_article["width"])
    ]
    slot_article["units_per_position"] = slot_article["stock_pal_total"].clip(lower=1)

    support_article_owner = (
        slot_article.groupby(["article", "owner"], dropna=False)
        .agg(
            observed_positions=("ubicacion", "nunique"),
            observed_units=("stock_pal_total", "sum"),
            units_per_position=("units_per_position", "median"),
            tipologia_estimacion=("tipologia", lambda series: series.mode().iat[0] if not series.mode().empty else series.iloc[0]),
            width_mode=("width", lambda series: series.mode().iat[0] if not series.mode().empty else series.iloc[0]),
            eu_eq_factor=("eu_equivalente_factor", "median"),
            support_rows=("ubicacion", "count"),
        )
        .reset_index()
    )
    support_article = (
        slot_article.groupby("article", dropna=False)
        .agg(
            observed_positions=("ubicacion", "nunique"),
            observed_units=("stock_pal_total", "sum"),
            units_per_position=("units_per_position", "median"),
            tipologia_estimacion=("tipologia", lambda series: series.mode().iat[0] if not series.mode().empty else series.iloc[0]),
            width_mode=("width", lambda series: series.mode().iat[0] if not series.mode().empty else series.iloc[0]),
            eu_eq_factor=("eu_equivalente_factor", "median"),
            support_rows=("ubicacion", "count"),
        )
        .reset_index()
    )
    support_tipology = (
        slot_article.groupby("tipologia", dropna=False)
        .agg(
            units_per_position=("units_per_position", "median"),
            eu_eq_factor=("eu_equivalente_factor", "median"),
            support_rows=("ubicacion", "count"),
        )
        .reset_index()
    )
    return support_article_owner, support_article, support_tipology


def _infer_tipology_from_dimensions(row: pd.Series) -> tuple[str | None, str]:
    max_dimension = row.get("max_dimension_cm")
    volume = row.get("m3")
    kilos = row.get("kilos")
    category = _normalize_text(row.get("categoria"))

    if pd.notna(max_dimension):
        if max_dimension >= 250:
            return "suelo_300", "dimensión >= 250 cm"
        if max_dimension >= 190 or "EXTERIOR" in category or "EXTERIORES" in category:
            return "suelo_250", "dimensión >= 190 cm o categoría exterior"
        if max_dimension <= 40 and pd.notna(volume) and volume <= 0.08 and (pd.isna(kilos) or kilos <= 10):
            return "balda_9h", "dimensión pequeña con bajo volumen"
        if max_dimension <= 126 and pd.notna(volume) and volume <= 0.8:
            return "suelo_126", "dimensión <= 126 cm"
        if max_dimension <= 130:
            return "EU", "dimensión compatible con EU"
        if max_dimension <= 180:
            return "AM", "dimensión compatible con AM"
        return "TR", "dimensión larga compatible con TR"

    return None, "sin dimensiones suficientes"


def _external_owner_assignment(external: pd.DataFrame, owner_map: pd.DataFrame) -> pd.DataFrame:
    frame = external.copy()
    frame["art"] = frame["art"].map(_canonical_identifier)
    frame["reserva"] = frame["reserva"].map(_normalize_text)
    frame["uds"] = pd.to_numeric(frame["uds"], errors="coerce")
    frame["qcnt"] = pd.to_numeric(frame["qcnt"], errors="coerce")
    frame["adr"] = frame["adr"].map(_normalize_text)

    owner_lookup = (
        owner_map.dropna(subset=["departamento_key", "propietario_num"])
        .set_index("departamento_key")["propietario_num"]
        .astype(int)
        .to_dict()
    )

    assigned_owner: list[object] = []
    assignment_support: list[str] = []
    for reserva in frame["reserva"]:
        owner = owner_lookup.get(reserva.upper())
        if owner is not None:
            assigned_owner.append(owner)
            assignment_support.append("propietario_departamento.xlsx")
            continue
        prefix = _owner_prefix_from_reserva(reserva)
        if prefix is not None:
            assigned_owner.append(prefix)
            assignment_support.append("prefijo_RESERVA")
            continue
        assigned_owner.append("sin_asignacion_segura")
        assignment_support.append("sin_match")

    frame["owner_base"] = assigned_owner
    frame["owner_assignment_support"] = assignment_support
    return frame


def _estimate_external_stock(
    external: pd.DataFrame,
    dimensions: pd.DataFrame,
    support_article_owner: pd.DataFrame,
    support_article: pd.DataFrame,
    support_tipology: pd.DataFrame,
) -> pd.DataFrame:
    dimensions_lookup = dimensions.drop_duplicates("article").set_index("article")
    support_article_owner_lookup = support_article_owner.set_index(["article", "owner"])
    support_article_lookup = support_article.set_index("article")
    support_tipology_lookup = support_tipology.set_index("tipologia")

    records: list[dict[str, object]] = []
    for row in external.itertuples(index=False):
        article = row.art
        owner = row.owner_base
        uds = row.uds if pd.notna(row.uds) else row.qcnt
        dims_row = dimensions_lookup.loc[article] if article in dimensions_lookup.index else None

        layer = None
        support_detail = None
        tipology = None
        factor = None
        eu_eq_factor = None
        source_width = None

        if isinstance(owner, (int, float)) and (article, int(owner)) in support_article_owner_lookup.index:
            support = support_article_owner_lookup.loc[(article, int(owner))]
            layer = "A"
            support_detail = "match_articulo_propietario"
            tipology = str(support["tipologia_estimacion"])
            factor = float(max(support["units_per_position"], 1.0))
            eu_eq_factor = float(max(support["eu_eq_factor"], 1.0 / 9.0))
            source_width = _normalize_text(support.get("width_mode", ""))
        elif article in support_article_lookup.index:
            support = support_article_lookup.loc[article]
            layer = "B"
            support_detail = "match_articulo"
            tipology = str(support["tipologia_estimacion"])
            factor = float(max(support["units_per_position"], 1.0))
            eu_eq_factor = float(max(support["eu_eq_factor"], 1.0 / 9.0))
            source_width = _normalize_text(support.get("width_mode", ""))
        else:
            inferred_tipology, inferred_support = _infer_tipology_from_dimensions(dims_row if dims_row is not None else pd.Series(dtype=object))
            if inferred_tipology is None:
                layer = "C"
                support_detail = "revision_manual_sin_dimensiones"
            else:
                tipology = inferred_tipology
                support_detail = inferred_support
                layer = "C"
                if tipology in support_tipology_lookup.index:
                    support = support_tipology_lookup.loc[tipology]
                    factor = float(max(support["units_per_position"], 1.0))
                    eu_eq_factor = float(max(support["eu_eq_factor"], 1.0 / 9.0))
                else:
                    factor = 1.0
                    eu_eq_factor = _tipology_to_eu_eq(tipology)

        unit_result = _tipology_unit(tipology) if tipology else "revision_manual"
        status = "revision_manual"
        positions_estimated = pd.NA
        if tipology and factor and pd.notna(uds):
            positions_estimated = float(max(uds / factor, 0.0))
            if layer in {"A", "B"}:
                status = "soportado"
            elif layer == "C":
                status = "inferido"

        if unit_result == "subhueco_balda" and pd.notna(positions_estimated):
            eu_equivalente = float(positions_estimated) * (1.0 / 9.0)
        elif pd.notna(positions_estimated):
            eu_equivalente = float(positions_estimated) * float(eu_eq_factor if eu_eq_factor is not None else 1.0)
        else:
            eu_equivalente = pd.NA

        if pd.notna(positions_estimated):
            modulos_3eu = _tipology_to_modules(float(positions_estimated), tipology, source_width)
        else:
            modulos_3eu = pd.NA

        records.append(
            {
                "art": article,
                "reserva": row.reserva,
                "owner_base": owner,
                "uds": row.uds,
                "adr": row.adr,
                "qcnt": row.qcnt,
                "capa_conversion": layer or "C",
                "factor_conversion_usado": factor if factor is not None else pd.NA,
                "tipologia_estimacion": tipology if tipology else "revision_manual",
                "unidad_resultante": unit_result,
                "posiciones_estimadas": positions_estimated,
                "soporte": f"{support_detail}; owner={row.owner_assignment_support}",
                "estado_conversion": status,
                "eu_equivalente": eu_equivalente,
                "modulos_3eu": modulos_3eu,
                "penaliza_altura_10": tipology in PENALIZES_HEIGHT_10 if tipology else False,
                "impacto_10": eu_equivalente if tipology in PENALIZES_HEIGHT_10 and pd.notna(eu_equivalente) else 0.0,
            }
        )

    return pd.DataFrame.from_records(records)


def _owner_metrics(movements: pd.DataFrame, requests: pd.DataFrame) -> pd.DataFrame:
    movement_metrics = (
        movements.groupby("owner", dropna=False)
        .agg(
            salidas_lineas=("transaction_id", "count"),
            salidas_transacciones=("transaction_id", "nunique"),
            volumen_unidades=("cantidad", "sum"),
            articulos_con_salida=("article", "nunique"),
            fecha_ultima_salida=("fecha_finalizacion", "max"),
        )
        .reset_index()
    )

    request_metrics = (
        requests.groupby("owner", dropna=False)
        .agg(
            servicios_lineas=("pedido", "count"),
            servicios_unidades=("cantidad_solicitada", "sum"),
            servicios_ultimos=("fecha_servicio", "max"),
        )
        .reset_index()
    )

    combined = movement_metrics.merge(request_metrics, on="owner", how="left")
    for column in [
        "salidas_lineas",
        "salidas_transacciones",
        "volumen_unidades",
        "articulos_con_salida",
        "servicios_lineas",
        "servicios_unidades",
    ]:
        combined[column] = combined[column].fillna(0.0)

    score_inputs = combined[["salidas_transacciones", "salidas_lineas", "volumen_unidades", "servicios_lineas"]].copy()
    score_inputs = score_inputs.replace(0, np.nan)
    normalized = (score_inputs.fillna(0) - score_inputs.fillna(0).min()) / (score_inputs.fillna(0).max() - score_inputs.fillna(0).min()).replace(0, 1)
    combined["ranking_operativo_score"] = (
        normalized["salidas_transacciones"] * 0.45
        + normalized["salidas_lineas"] * 0.25
        + normalized["volumen_unidades"] * 0.20
        + normalized["servicios_lineas"] * 0.10
    )
    return combined.sort_values(["ranking_operativo_score", "salidas_transacciones"], ascending=[False, False])


def _build_demand_tables(
    slot_level: pd.DataFrame,
    external_equivalences: pd.DataFrame,
    owner_metrics: pd.DataFrame,
    owner_names: dict[int, str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    current_assignable = slot_level.copy()
    current_assignable["owner_bucket"] = current_assignable["owner_bucket"].astype(str)
    current_counts = (
        current_assignable.groupby(["owner_bucket", "tipologia"], dropna=False)
        .size()
        .unstack(fill_value=0)
        .reset_index()
        .rename(columns={"owner_bucket": "propietario"})
    )
    current_counts["propietario"] = current_counts["propietario"].astype(str)

    current_eq = (
        current_assignable.assign(eu_eq=current_assignable["eu_equivalente_factor"])
        .groupby("owner_bucket", dropna=False)["eu_eq"]
        .sum()
        .reset_index()
        .rename(columns={"owner_bucket": "propietario", "eu_eq": "capacidad_actual"})
    )
    current_eq["propietario"] = current_eq["propietario"].astype(str)

    current_penalty = (
        current_assignable[current_assignable["tipologia"].isin(PENALIZES_HEIGHT_10)]
        .groupby("owner_bucket", dropna=False)["eu_equivalente_factor"]
        .sum()
        .reset_index()
        .rename(columns={"owner_bucket": "propietario", "eu_equivalente_factor": "penalty_10_actual"})
    )
    current_penalty["propietario"] = current_penalty["propietario"].astype(str)

    external_group = (
        external_equivalences.groupby(["owner_base", "estado_conversion"], dropna=False)["eu_equivalente"]
        .sum(min_count=1)
        .unstack(fill_value=0)
        .reset_index()
        .rename(columns={"owner_base": "propietario"})
    )
    external_group["propietario"] = external_group["propietario"].astype(str)
    for column in ["soportado", "inferido", "revision_manual"]:
        external_group[column] = external_group.get(column, 0.0)

    external_penalty = (
        external_equivalences.groupby(["owner_base", "estado_conversion"], dropna=False)["impacto_10"]
        .sum(min_count=1)
        .unstack(fill_value=0)
        .reset_index()
        .rename(
            columns={
                "owner_base": "propietario",
                "soportado": "penalty_10_externo_soportado",
                "inferido": "penalty_10_externo_inferido",
                "revision_manual": "penalty_10_externo_manual",
            }
        )
    )
    external_penalty["propietario"] = external_penalty["propietario"].astype(str)
    for column in ["penalty_10_externo_soportado", "penalty_10_externo_inferido", "penalty_10_externo_manual"]:
        external_penalty[column] = external_penalty.get(column, 0.0)

    demand = (
        current_counts
        .merge(current_eq, on="propietario", how="outer")
        .merge(current_penalty, on="propietario", how="outer")
        .merge(external_group, on="propietario", how="outer")
        .merge(external_penalty, on="propietario", how="outer")
    )
    numeric_fill_columns = [
        column
        for column in demand.columns
        if column != "propietario"
    ]
    for column in numeric_fill_columns:
        demand[column] = pd.to_numeric(demand[column], errors="coerce").fillna(0)
    demand = demand.infer_objects(copy=False)
    demand["owner_numeric"] = pd.to_numeric(demand["propietario"], errors="coerce")
    demand = demand.merge(
        owner_metrics.rename(columns={"owner": "owner_numeric", "ranking_operativo_score": "ranking_operativo_score"}),
        on="owner_numeric",
        how="left",
    )
    demand["owner_name"] = demand["owner_numeric"].map(lambda value: owner_names.get(int(value), str(int(value))) if pd.notna(value) else "SIN_ASIGNACION_SEGURA")

    for column in ["EU", "AM", "TR", "suelo_estandar", "suelo_250", "suelo_300", "suelo_126", "balda_9h"]:
        if column not in demand.columns:
            demand[column] = 0.0

    demand["demanda_actual_eu_eq"] = (
        demand["EU"] * 1.0
        + demand["AM"] * 1.5
        + demand["TR"] * 3.0
        + demand["suelo_estandar"] * 1.0
        + demand["suelo_250"] * 1.0
        + demand["suelo_300"] * 1.0
        + demand["suelo_126"] * 1.0
        + demand["balda_9h"] * (1.0 / 9.0)
    )
    demand["externo_soportado_total"] = demand["soportado"]
    demand["externo_inferido_total"] = demand["inferido"]
    demand["externo_revision_manual"] = demand["revision_manual"]
    demand["penalty_10_total_base"] = demand["penalty_10_actual"] + demand["penalty_10_externo_soportado"]
    demand["penalty_10_total_conditioned"] = demand["penalty_10_total_base"] + demand["penalty_10_externo_inferido"]
    demand["total_recomendado_90"] = (demand["demanda_actual_eu_eq"] + demand["externo_soportado_total"]) / SCENARIO_RECOMMENDED
    demand = demand.sort_values(["ranking_operativo_score", "demanda_actual_eu_eq"], ascending=[False, False]).reset_index(drop=True)

    demand_table = pd.DataFrame(
        {
            "propietario": demand["propietario"],
            "demanda_actual_eu": demand["EU"],
            "demanda_actual_am": demand["AM"],
            "demanda_actual_tr": demand["TR"],
            "suelo_estandar_actual": demand["suelo_estandar"],
            "suelo_250_actual": demand["suelo_250"],
            "suelo_300_actual": demand["suelo_300"],
            "suelo_126_actual": demand["suelo_126"],
            "balda_9h_actual": demand["balda_9h"],
            "externo_soportado_total": demand["externo_soportado_total"],
            "externo_inferido_total": demand["externo_inferido_total"],
            "externo_revision_manual": demand["externo_revision_manual"],
            "total_recomendado_90": demand["total_recomendado_90"],
        }
    )
    return demand, demand_table


def _conversion_table_from_current(
    slot_level: pd.DataFrame,
    external_equivalences: pd.DataFrame,
) -> pd.DataFrame:
    current_conversion = (
        slot_level.groupby(["tipologia", "unit_resultante"], dropna=False)
        .agg(
            valor_raw=("ubicacion", "nunique"),
            eu_equivalente=("eu_equivalente_factor", "sum"),
        )
        .reset_index()
    )
    supported_external = (
        external_equivalences[external_equivalences["estado_conversion"] != "revision_manual"]
        .groupby(["tipologia_estimacion", "unidad_resultante"], dropna=False)
        .agg(
            valor_raw=("posiciones_estimadas", "sum"),
            eu_equivalente=("eu_equivalente", "sum"),
        )
        .reset_index()
        .rename(columns={"tipologia_estimacion": "tipologia"})
    )

    rows: list[dict[str, object]] = []
    for source, frame in [("actual", current_conversion), ("externo", supported_external)]:
        for row in frame.itertuples(index=False):
            tipology = str(row.tipologia)
            eu_eq = float(row.eu_equivalente) if pd.notna(row.eu_equivalente) else 0.0
            rows.append(
                {
                    "tipologia": f"{tipology}_{source}",
                    "valor_raw": row.valor_raw,
                    "unidad_raw": getattr(row, "unidad_resultante", getattr(row, "unit_resultante", "unidad_desconocida")),
                    "modulos_3eu": eu_eq / 3.0 if tipology != "balda_9h" else float(row.valor_raw) / 27.0,
                    "eu_equivalente": eu_eq,
                    "penaliza_altura_10": tipology in PENALIZES_HEIGHT_10,
                    "impacto_10": eu_eq if tipology in PENALIZES_HEIGHT_10 else 0.0,
                    "soporte": source,
                }
            )
    return pd.DataFrame.from_records(rows)


def _reconciliation_table(
    study_values: pd.DataFrame,
    slot_level: pd.DataFrame,
) -> pd.DataFrame:
    slot_summary = (
        slot_level.groupby("tipologia", dropna=False)
        .agg(
            codex_posiciones=("ubicacion", "nunique"),
            codex_eu_equivalente=("eu_equivalente_factor", "sum"),
        )
        .reset_index()
    )
    codex_records = []
    for row in slot_summary.itertuples(index=False):
        tipology = str(row.tipologia)
        if tipology == "balda_9h":
            codex_records.append((f"{tipology}_valor_raw", row.codex_posiciones, "subhueco_balda"))
            codex_records.append((f"{tipology}_modulos_3eu", row.codex_posiciones / 27.0, "modulo_3eu"))
        else:
            codex_records.append((f"{tipology}_valor_raw", row.codex_posiciones, "posicion_suelo" if tipology.startswith("suelo") else "posicion_rack"))
            codex_records.append((f"{tipology}_modulos_3eu", row.codex_eu_equivalente / 3.0, "modulo_3eu"))

    codex_df = pd.DataFrame(codex_records, columns=["tipologia", "codex_recalculado_valor", "codex_recalculado_unidad"])
    reconciled = study_values.merge(codex_df, on="tipologia", how="outer")

    comments = []
    for row in reconciled.itertuples(index=False):
        comment = "recalculado desde slot físico consolidado"
        if "balda_9h" in str(row.tipologia):
            comment = "1 módulo 3EU equivale a 27 subhuecos; los valores 180/557 mezclan unidades."
        elif "suelo_250" in str(row.tipologia):
            comment = "252 sale a nivel fila; Codex usa unidad física consolidada y convierte con ancho observado."
        elif "suelo_126" in str(row.tipologia):
            comment = "el documento avanzado mezcla posiciones y módulos; Codex mantiene posición_suelo separada."
        reconciled.loc[reconciled["tipologia"] == row.tipologia, "comentario_diferencia"] = comment

    return reconciled[
        [
            "tipologia",
            "study_previo_valor",
            "study_previo_unidad",
            "investigacion_avanzada_valor",
            "investigacion_avanzada_unidad",
            "codex_recalculado_valor",
            "codex_recalculado_unidad",
            "comentario_diferencia",
        ]
    ]


def _external_summary_table(external_assigned: pd.DataFrame) -> pd.DataFrame:
    adr_repeated_ratio = external_assigned["adr"].duplicated(keep=False).mean()
    qcnt_eq_uds_ratio = (external_assigned["qcnt"] == external_assigned["uds"]).mean()
    conclusion = "granularidad heterogénea"
    risk = "alto"
    if adr_repeated_ratio < 0.2 and qcnt_eq_uds_ratio > 0.9:
        conclusion = "granularidad homogénea"
        risk = "bajo"

    return pd.DataFrame(
        [
            {
                "filas_totales": len(external_assigned),
                "filas_con_owner_asignado": int(external_assigned["owner_base"].astype(str).ne("sin_asignacion_segura").sum()),
                "filas_sin_owner_seguro": int(external_assigned["owner_base"].astype(str).eq("sin_asignacion_segura").sum()),
                "ratio_ADR_repetido": adr_repeated_ratio,
                "ratio_QCNT_eq_UDS": qcnt_eq_uds_ratio,
                "conclusion_granularidad": conclusion,
                "riesgo": risk,
            }
        ]
    )


def _impact_table(demand: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "propietario": demand["propietario"],
            "capacidad_actual": demand["demanda_actual_eu_eq"],
            "externo_soportado": demand["externo_soportado_total"],
            "externo_inferido": demand["externo_inferido_total"],
            "revision_manual": demand["externo_revision_manual"],
            "delta_total": demand["externo_soportado_total"] + demand["externo_inferido_total"] + demand["externo_revision_manual"],
        }
    )


def _assign_owners_to_aisles(
    demand: pd.DataFrame,
    owner_names: dict[int, str],
    scenario: str,
    include_inferred: bool,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    planning = demand.copy()
    planning = planning[planning["propietario"].astype(str) != "sin_asignacion_segura"].copy()
    planning["owner_numeric"] = planning["owner_numeric"].astype(int)
    planning["required_eq"] = planning["demanda_actual_eu_eq"] + planning["externo_soportado_total"]
    if include_inferred:
        planning["required_eq"] = planning["required_eq"] + planning["externo_inferido_total"]

    planning["required_eq_90"] = planning["required_eq"] / SCENARIO_RECOMMENDED
    planning["special_penalty_10"] = planning["penalty_10_total_base"]
    if include_inferred:
        planning["special_penalty_10"] = planning["penalty_10_total_conditioned"]

    planning = planning.sort_values(["ranking_operativo_score", "required_eq_90"], ascending=[False, False]).reset_index(drop=True)

    aisle_records: list[dict[str, object]] = []
    assignment_records: list[dict[str, object]] = []
    aisle_usage = {aisle: 0.0 for aisle in DESTINATION_AISLES}
    aisle_penalty = {aisle: 0.0 for aisle in DESTINATION_AISLES}
    current_aisle = 1

    for row in planning.itertuples(index=False):
        remaining = float(row.required_eq_90)
        owner_aisles: list[int] = []
        owner_penalty_remaining = float(row.special_penalty_10)

        while remaining > 1e-9 and current_aisle <= DESTINATION_AISLES[-1]:
            available = DESTINATION_THEORETICAL_CAPACITY * SCENARIO_RECOMMENDED - aisle_usage[current_aisle]
            if available <= 1e-9:
                current_aisle += 1
                continue
            allocated = min(available, remaining)
            aisle_usage[current_aisle] += allocated
            owner_aisles.append(current_aisle)
            proportional_penalty = min(owner_penalty_remaining, allocated)
            aisle_penalty[current_aisle] += proportional_penalty
            owner_penalty_remaining -= proportional_penalty
            remaining -= allocated
            if aisle_usage[current_aisle] >= DESTINATION_THEORETICAL_CAPACITY * SCENARIO_RECOMMENDED - 1e-9:
                current_aisle += 1

        principal = owner_aisles[0] if owner_aisles else pd.NA
        secondary = owner_aisles[1] if len(owner_aisles) > 1 else pd.NA
        assignment_type = "dedicado" if len(owner_aisles) == 1 else "split_contiguo"
        support = f"ranking={row.ranking_operativo_score:.3f}; demanda_90={row.required_eq_90:.1f}"
        justification = "propietario priorizado por salidas históricas, volumen y capacidad" if len(owner_aisles) <= 2 else "requiere bloque ampliado"
        assignment_records.append(
            {
                "propietario": row.propietario,
                "ranking_operativo": round(float(row.ranking_operativo_score), 4),
                "pasillo_principal": principal,
                "pasillo_secundario": secondary,
                "tipo_asignacion": assignment_type,
                "escenario": scenario,
                "soporte_asignacion": support,
                "justificacion": justification,
            }
        )

    for aisle in DESTINATION_AISLES:
        aisle_records.append(
            {
                "pasillo_destino": aisle,
                "capacidad_teorica": DESTINATION_THEORETICAL_CAPACITY,
                "capacidad_estandar_00": DESTINATION_HEIGHT_CAPACITY,
                "capacidad_estandar_10": DESTINATION_HEIGHT_CAPACITY,
                "capacidad_estandar_20": DESTINATION_HEIGHT_CAPACITY,
                "capacidad_estandar_30": DESTINATION_HEIGHT_CAPACITY,
                "capacidad_estandar_40": DESTINATION_HEIGHT_CAPACITY,
                "penalizacion_10": round(aisle_penalty[aisle], 2),
                "capacidad_util_resultante": round(DESTINATION_THEORETICAL_CAPACITY - aisle_penalty[aisle], 2),
            }
        )

    return pd.DataFrame(assignment_records), pd.DataFrame(aisle_records)


def _build_conflict_table(conflicts: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for row in conflicts.itertuples(index=False):
        if row.owner_count > 1:
            rows.append(
                {
                    "articulo": ", ".join(row.articles[:3]),
                    "propietario": " / ".join(str(owner) for owner in row.owners),
                    "motivo_conflicto": "ubicacion_compartida_multiowner",
                    "evidencia": f"{row.ubicacion} con {row.owner_count} propietarios",
                    "accion_recomendada": "romper mezcla en layout destino",
                }
            )
        elif row.article_count > 1:
            rows.append(
                {
                    "articulo": ", ".join(row.articles[:3]),
                    "propietario": row.owners[0] if row.owners else "sin_asignacion_segura",
                    "motivo_conflicto": "multireferencia_mismo_slot",
                    "evidencia": f"{row.ubicacion} con {row.article_count} artículos",
                    "accion_recomendada": "normalizar a una referencia por unidad física cuando sea crítico",
                }
            )
    return pd.DataFrame(rows)


def _build_suppositions_table() -> pd.DataFrame:
    rows = [
        {
            "supuesto": "El doc descargado se trata como estudio_previo y el doc del workspace como investigación_avanzada",
            "motivo": "son los dos estudios disponibles con narrativa y tablas de dimensionamiento",
            "impacto": "afecta la conciliación entre estudios",
            "sensibilidad": "media",
            "estado": "explicito",
        },
        {
            "supuesto": "S y 00 se convierten a 1 EU-equivalente cuando no hay ancho observado adicional",
            "motivo": "son posiciones de suelo sin conversión de ancho explícita",
            "impacto": "afecta penalización 10 y demanda EU-equivalente",
            "sensibilidad": "alta",
            "estado": "explicito",
        },
        {
            "supuesto": "1 módulo de balda 9h equivale a 27 subhuecos",
            "motivo": "cada módulo 3EU aporta 3 anchos EU y cada ancho se divide en 9",
            "impacto": "corrige de forma material los 180/557 módulos de los informes",
            "sensibilidad": "alta",
            "estado": "explicito",
        },
        {
            "supuesto": "lineas_solicitudes_con_pedidos actúa como sanity check operativo y no como fuente principal de capacidad",
            "motivo": "el usuario lo pidió como apoyo secundario",
            "impacto": "afecta solo el ranking combinado, no la capacidad base",
            "sensibilidad": "baja",
            "estado": "explicito",
        },
        {
            "supuesto": "el stock externo se considera incremental, pero se deja validación de solape por artículo-propietario",
            "motivo": "no existe clave física común entre ambos sistemas",
            "impacto": "riesgo de doble conteo si el externo no es realmente externo",
            "sensibilidad": "alta",
            "estado": "condicionado",
        },
    ]
    return pd.DataFrame(rows)


def _scenario_table(
    demand: pd.DataFrame,
    aisle_capacity: pd.DataFrame,
    include_inferred: bool,
) -> pd.DataFrame:
    capacity_util = aisle_capacity["capacidad_util_resultante"].sum()
    current = demand["demanda_actual_eu_eq"].sum()
    supported = current + demand["externo_soportado_total"].sum()
    total = supported + demand["externo_inferido_total"].sum() + demand["externo_revision_manual"].sum()

    rows = []
    for label, objective in [("base_90", SCENARIO_RECOMMENDED), ("stress_95", SCENARIO_STRESS)]:
        target_capacity = capacity_util * objective
        if label == "base_90":
            demand_total = supported
        else:
            demand_total = total if include_inferred else supported
        rows.append(
            {
                "escenario": label,
                "ocupacion_objetivo": objective,
                "capacidad_util": capacity_util,
                "demanda_actual": current,
                "demanda_con_externo_soportado": supported,
                "demanda_con_externo_total": total,
                "margen": target_capacity - demand_total,
            }
        )
    return pd.DataFrame(rows)


def _capacity_validation_table(
    aisle_capacity: pd.DataFrame,
    demand: pd.DataFrame,
) -> pd.DataFrame:
    penalty = aisle_capacity["penalizacion_10"].sum()
    capacity_result = DESTINATION_THEORETICAL_CAPACITY * len(DESTINATION_AISLES) - penalty
    demand_total = demand["demanda_actual_eu_eq"].sum() + demand["externo_soportado_total"].sum()
    return pd.DataFrame(
        [
            {
                "capacidad_teorica_total": DESTINATION_THEORETICAL_CAPACITY * len(DESTINATION_AISLES),
                "penalizacion_10": penalty,
                "capacidad_estandar_resultante": capacity_result,
                "capacidad_especial": demand[["suelo_250", "suelo_300", "suelo_126"]].sum().sum(),
                "demanda_total": demand_total,
                "cabe": demand_total <= capacity_result * SCENARIO_RECOMMENDED,
                "observacion": "la capacidad especial está incluida en la huella total; no se suma aparte",
            }
        ]
    )


def _validation_table(
    external_summary: pd.DataFrame,
    study_reconciliation: pd.DataFrame,
    slot_level: pd.DataFrame,
    external_equivalences: pd.DataFrame,
    assignment_base: pd.DataFrame,
    assignment_conditioned: pd.DataFrame,
) -> pd.DataFrame:
    overlap_ratio = (
        external_equivalences["art"].astype(str)
        .isin(slot_level["ubicacion"].astype(str))
        .mean()
    )
    changes = assignment_base.merge(
        assignment_conditioned[["propietario", "pasillo_principal"]].rename(columns={"pasillo_principal": "pasillo_principal_cond"}),
        on="propietario",
        how="outer",
    )
    changed_blocks = int((changes["pasillo_principal"] != changes["pasillo_principal_cond"]).fillna(False).sum())

    rows = [
        {
            "validacion": "granularidad_stock_externo",
            "resultado": external_summary.iloc[0]["conclusion_granularidad"],
            "detalle": f"ADR repetido={external_summary.iloc[0]['ratio_ADR_repetido']:.2%}; QCNT=UDS={external_summary.iloc[0]['ratio_QCNT_eq_UDS']:.2%}",
        },
        {
            "validacion": "conciliacion_entre_estudios",
            "resultado": "ok",
            "detalle": f"{len(study_reconciliation)} filas de conciliación con diferencias comentadas",
        },
        {
            "validacion": "unidades_tipologias_especiales",
            "resultado": "ok",
            "detalle": "posicion_suelo, subhueco_balda, modulo_3eu y eu_equivalente separados",
        },
        {
            "validacion": "penalizacion_altura_10",
            "resultado": "ok",
            "detalle": f"solo penalizan suelo_250 y suelo_300; slots afectados={int(slot_level['tipologia'].isin(PENALIZES_HEIGHT_10).sum())}",
        },
        {
            "validacion": "doble_conteo_stock_actual_vs_externo",
            "resultado": "riesgo_bajo_medio",
            "detalle": f"sin clave física común; solape físico directo no demostrado; ratio dummy={overlap_ratio:.2%}",
        },
        {
            "validacion": "propietarios_que_cambian_de_bloque",
            "resultado": changed_blocks,
            "detalle": f"propietarios con cambio de pasillo principal al incluir inferido={changed_blocks}",
        },
    ]
    return pd.DataFrame(rows)


def _plot_heatmap(data: pd.DataFrame, index: str, columns: str, values: str, title: str, path: Path) -> None:
    matrix = data.pivot_table(index=index, columns=columns, values=values, aggfunc="sum", fill_value=0)
    fig, ax = plt.subplots(figsize=(10, max(4, len(matrix) * 0.35)))
    image = ax.imshow(matrix.values, aspect="auto", cmap="YlGnBu")
    ax.set_title(title)
    ax.set_xticks(range(len(matrix.columns)))
    ax.set_xticklabels(matrix.columns, rotation=45, ha="right")
    ax.set_yticks(range(len(matrix.index)))
    ax.set_yticklabels(matrix.index)
    fig.colorbar(image, ax=ax)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_rankings(demand: pd.DataFrame, owner_names: dict[int, str], path: Path) -> None:
    subset = demand.head(15).copy()
    labels = []
    for row in subset.itertuples(index=False):
        owner = int(row.owner_numeric) if pd.notna(row.owner_numeric) else -1
        labels.append(f"{owner} {owner_names.get(owner, '')}".strip())
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(labels, subset["ranking_operativo_score"], color="#0b7285")
    ax.set_title("Ranking combinado de propietarios")
    ax.set_ylabel("score combinado")
    ax.tick_params(axis="x", rotation=75)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_waterfall(capacity_validation: pd.DataFrame, demand: pd.DataFrame, path: Path) -> None:
    row = capacity_validation.iloc[0]
    values = [
        row["capacidad_teorica_total"],
        -row["penalizacion_10"],
        row["capacidad_estandar_resultante"],
        -demand["demanda_actual_eu_eq"].sum(),
        -demand["externo_soportado_total"].sum(),
        row["capacidad_estandar_resultante"] - demand["demanda_actual_eu_eq"].sum() - demand["externo_soportado_total"].sum(),
    ]
    labels = [
        "Cap. teórica",
        "Penalización 10",
        "Cap. útil",
        "Demanda actual",
        "Externo soportado",
        "Buffer",
    ]
    cumulative = np.cumsum([0] + values[:-1])
    fig, ax = plt.subplots(figsize=(11, 5))
    colors = ["#2f9e44", "#c92a2a", "#1971c2", "#e67700", "#e67700", "#2b8a3e"]
    for idx, (label, value, start) in enumerate(zip(labels, values, cumulative)):
        bottom = min(start, start + value)
        ax.bar(idx, abs(value), bottom=bottom, color=colors[idx])
        ax.text(idx, bottom + abs(value) + 20, f"{value:,.1f}", ha="center", fontsize=8)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=20, ha="right")
    ax.set_title("Waterfall capacidad -> demanda")
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_modules(conversion_table: pd.DataFrame, path: Path) -> None:
    subset = conversion_table[conversion_table["tipologia"].str.endswith("_actual")].copy()
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(subset["tipologia"], subset["modulos_3eu"], color="#5f3dc4")
    ax.set_title("Módulos a convertir por tipología")
    ax.tick_params(axis="x", labelrotation=45)
    plt.setp(ax.get_xticklabels(), ha="right")
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_external_impact(impact: pd.DataFrame, path: Path) -> None:
    subset = impact.sort_values("delta_total", ascending=False).head(15)
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(subset["propietario"].astype(str), subset["delta_total"], color="#f08c00")
    ax.set_title("Impacto incremental del stock externo por propietario")
    ax.set_ylabel("EU-equivalente")
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_external_coverage(external_equivalences: pd.DataFrame, path: Path) -> None:
    counts = external_equivalences["estado_conversion"].value_counts()
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.pie(counts.values, labels=counts.index, autopct="%1.1f%%", startangle=90)
    ax.set_title("Cobertura de stock externo")
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_logical_layout(assignments: pd.DataFrame, title: str, path: Path) -> None:
    ordered = assignments.sort_values("pasillo_principal")
    fig, ax = plt.subplots(figsize=(12, 2.8))
    for idx, row in enumerate(ordered.itertuples(index=False)):
        if pd.isna(row.pasillo_principal):
            continue
        ax.barh(0, 0.8, left=float(row.pasillo_principal) - 0.4, height=0.35, color="#1971c2")
        ax.text(float(row.pasillo_principal), 0, str(row.propietario), ha="center", va="center", color="white", fontsize=8)
        if pd.notna(row.pasillo_secundario):
            ax.barh(0, 0.8, left=float(row.pasillo_secundario) - 0.4, height=0.35, color="#4dabf7")
            ax.text(float(row.pasillo_secundario), 0, str(row.propietario), ha="center", va="center", color="white", fontsize=8)
    ax.set_xlim(0.5, 12.5)
    ax.set_yticks([])
    ax.set_xticks(DESTINATION_AISLES)
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_conciliation(reconciliation: pd.DataFrame, path: Path) -> None:
    subset = reconciliation[reconciliation["codex_recalculado_valor"].notna()].head(12).copy()
    fig, ax = plt.subplots(figsize=(12, 6))
    x = np.arange(len(subset))
    width = 0.25
    prev = pd.to_numeric(subset["study_previo_valor"], errors="coerce").fillna(0)
    adv = pd.to_numeric(subset["investigacion_avanzada_valor"], errors="coerce").fillna(0)
    codex = pd.to_numeric(subset["codex_recalculado_valor"], errors="coerce").fillna(0)
    ax.bar(x - width, prev, width=width, label="Estudio previo", color="#868e96")
    ax.bar(x, adv, width=width, label="Investigación avanzada", color="#f03e3e")
    ax.bar(x + width, codex, width=width, label="Codex", color="#2b8a3e")
    ax.set_xticks(x)
    ax.set_xticklabels(subset["tipologia"], rotation=60, ha="right")
    ax.set_title("Conciliación entre estudios")
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _render_summary_markdown(
    paths: PipelinePaths,
    demand: pd.DataFrame,
    capacity_validation: pd.DataFrame,
    assignment_base: pd.DataFrame,
    assignment_conditioned: pd.DataFrame,
    external_summary: pd.DataFrame,
    validation_table: pd.DataFrame,
    reconciliation: pd.DataFrame,
    conversion_table: pd.DataFrame,
) -> None:
    row = capacity_validation.iloc[0]
    ext = external_summary.iloc[0]
    base_lines = assignment_base.sort_values("pasillo_principal").head(12)
    conditioned_lines = assignment_conditioned.sort_values("pasillo_principal").head(12)
    infra = conversion_table[
        conversion_table["tipologia"].isin(
            ["suelo_250_actual", "suelo_300_actual", "suelo_126_actual", "balda_9h_actual"]
        )
    ].copy()
    infra_map = infra.set_index("tipologia")

    def fmt_aisle(value: object) -> str:
        if pd.isna(value):
            return "sin_hueco_soportado"
        return str(int(value))

    lines = [
        "# 1. qué se recalculó",
        "",
        "- Se recalculó la ocupación actual desde `17-04-2026.xlsx` consolidando a unidad física ocupada y separando `posicion_suelo`, `subhueco_balda`, `modulo_3eu` y `eu_equivalente`.",
        "- Se reconstruyó el ranking operativo por propietario desde `movimientos.xlsx`, con `lineas_solicitudes_con_pedidos.xlsx` como sanity check secundario.",
        "- Se incorporó `STOCK_MAP_FACT_MAHOU_260415020005.xlsx` con conversión por capas A/B/C y validación explícita de granularidad.",
        "",
        "# 2. qué cifras del informe avanzado se corrigieron",
        "",
        f"- `suelo_250`: el avanzado mezcla `252 posiciones`, `121 módulos` y `84 módulos`; Codex recalcula {infra_map.loc['suelo_250_actual', 'valor_raw']:.0f} `posicion_suelo`, {infra_map.loc['suelo_250_actual', 'eu_equivalente']:.1f} `eu_equivalente` y {infra_map.loc['suelo_250_actual', 'modulos_3eu']:.2f} `modulo_3eu`.",
        f"- `suelo_300`: Codex recalcula {infra_map.loc['suelo_300_actual', 'valor_raw']:.0f} `posicion_suelo`, {infra_map.loc['suelo_300_actual', 'eu_equivalente']:.1f} `eu_equivalente` y {infra_map.loc['suelo_300_actual', 'modulos_3eu']:.2f} `modulo_3eu`.",
        f"- `suelo_126`: el avanzado mezcla `101 posiciones`, `40 módulos` y `34 módulos`; Codex recalcula {infra_map.loc['suelo_126_actual', 'valor_raw']:.0f} `posicion_suelo` y {infra_map.loc['suelo_126_actual', 'modulos_3eu']:.2f} `modulo_3eu`.",
        f"- `balda_9h`: el avanzado mezcla `540 subhuecos`, `557 módulos equivalentes` y `180 módulos`; Codex recalcula {infra_map.loc['balda_9h_actual', 'valor_raw']:.0f} `subhueco_balda`, {infra_map.loc['balda_9h_actual', 'eu_equivalente']:.2f} `eu_equivalente` y {infra_map.loc['balda_9h_actual', 'modulos_3eu']:.2f} `modulo_3eu`.",
        "",
        "# 3. qué está decisión-grade hoy",
        "",
        f"- Capacidad teórica destino: {row['capacidad_teorica_total']:.1f} EU-equivalente.",
        f"- Penalización recalculada de altura 10: {row['penalizacion_10']:.1f} EU-equivalente.",
        f"- Demanda base con externo soportado: {demand['demanda_actual_eu_eq'].sum() + demand['externo_soportado_total'].sum():.1f} EU-equivalente.",
        f"- Gap contra diseño 90%: {abs(pd.read_csv(paths.csv_dir / 'tabla_escenarios_ocupacion.csv').iloc[0]['margen']):.1f} EU-equivalente por cerrar o absorber con buffer.",
        f"- Con diseño al 90%, {'cabe' if bool(row['cabe']) else 'no cabe'} con el soporte actual.",
        "",
        "# 4. qué sigue condicionado por stock externo",
        "",
    ]
    lines.extend(
        [
            f"- Granularidad externo: {ext['conclusion_granularidad']} con riesgo {ext['riesgo']}.",
            f"- Ratio ADR repetido: {ext['ratio_ADR_repetido']:.2%}. Ratio QCNT=UDS: {ext['ratio_QCNT_eq_UDS']:.2%}.",
            "- Todo lo inferido por capa C se deja como condicionado y la revisión manual se mantiene fuera del cierre duro de layout.",
            "",
            "# 5. layout base recomendado",
            "",
        ]
    )
    for row_base in base_lines.itertuples(index=False):
        lines.append(
            f"- Propietario {row_base.propietario}: pasillo principal {fmt_aisle(row_base.pasillo_principal)}, secundario {fmt_aisle(row_base.pasillo_secundario)}, {row_base.tipo_asignacion}."
        )
    lines.extend(["", "# 6. layout condicionado", ""])
    for row_conditioned in conditioned_lines.itertuples(index=False):
        lines.append(
            f"- Propietario {row_conditioned.propietario}: pasillo principal {fmt_aisle(row_conditioned.pasillo_principal)}, secundario {fmt_aisle(row_conditioned.pasillo_secundario)}, {row_conditioned.tipo_asignacion}."
        )
    lines.extend(["", "# 7. necesidades mínimas de infra", ""])
    for item in infra.itertuples(index=False):
        lines.append(
            f"- {item.tipologia}: {item.modulos_3eu:.2f} módulos 3EU, {item.eu_equivalente:.2f} EU-equivalente, impacto_10={item.impacto_10:.2f}."
        )
    lines.extend(["", "# 8. riesgos y siguiente dato crítico a validar", ""])
    for item in validation_table.itertuples(index=False):
        lines.append(f"- {item.validacion}: {item.detalle}")

    (paths.output_dir / "resumen_codex.md").write_text("\n".join(lines), encoding="utf-8")


def _save_csv_tables(paths: PipelinePaths, tables: dict[str, pd.DataFrame]) -> None:
    for name, frame in tables.items():
        frame.to_csv(paths.csv_dir / f"{name}.csv", index=False)


def _save_support_tables(paths: PipelinePaths, tables: dict[str, pd.DataFrame]) -> None:
    for name, frame in tables.items():
        frame.to_csv(paths.support_dir / f"{name}.csv", index=False)


def _save_workbook_manifest(paths: PipelinePaths, tables: dict[str, pd.DataFrame]) -> None:
    payload = {}
    for name, frame in tables.items():
        export = frame.replace({pd.NA: None, np.nan: None}).to_dict(orient="records")
        payload[name] = export
    (paths.support_dir / "workbook_tables.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def run_mahou_dimensioning(base_dir: Path) -> dict[str, Path]:
    paths = _ensure_directories(base_dir)
    sources = _load_sources(base_dir)

    stock_rows, slot_article, slot_level, conflicts = _prepare_stock(sources["stock"])
    movements = _prepare_movements(sources["movements"])
    requests = _prepare_requests(sources["requests"])
    dimensions = _prepare_dimensions(sources["dimensions"])
    owner_map = _prepare_owner_map(sources["owner_map"])
    owner_metrics = _owner_metrics(movements, requests)
    support_article_owner, support_article, support_tipology = _current_article_support(slot_article)

    external_assigned = _external_owner_assignment(sources["external"], owner_map)
    external_equivalences = _estimate_external_stock(
        external_assigned,
        dimensions,
        support_article_owner,
        support_article,
        support_tipology,
    )
    owner_names = _owner_name_map(slot_article, owner_map)

    demand, demand_table = _build_demand_tables(slot_level, external_equivalences, owner_metrics, owner_names)
    impact_table = _impact_table(demand)
    conversion_table = _conversion_table_from_current(slot_level, external_equivalences)
    study_values = _extract_study_values(base_dir)
    reconciliation = _reconciliation_table(study_values, slot_level)
    external_summary = _external_summary_table(external_assigned)
    assignment_base, aisle_capacity_base = _assign_owners_to_aisles(demand, owner_names, "base", include_inferred=False)
    assignment_conditioned, aisle_capacity_conditioned = _assign_owners_to_aisles(demand, owner_names, "condicionado", include_inferred=True)
    conflict_table = _build_conflict_table(conflicts)
    suppositions = _build_suppositions_table()
    scenarios = _scenario_table(demand, aisle_capacity_base, include_inferred=True)
    capacity_validation = _capacity_validation_table(aisle_capacity_base, demand)
    validation_table = _validation_table(
        external_summary,
        reconciliation,
        slot_level,
        external_equivalences,
        assignment_base,
        assignment_conditioned,
    )

    stock_external_equivalences = external_equivalences.rename(
        columns={
            "art": "ART",
            "reserva": "RESERVA",
            "owner_base": "owner_base",
            "uds": "UDS",
            "adr": "ADR",
            "qcnt": "QCNT",
        }
    )[
        [
            "ART",
            "RESERVA",
            "owner_base",
            "UDS",
            "ADR",
            "QCNT",
            "capa_conversion",
            "factor_conversion_usado",
            "tipologia_estimacion",
            "unidad_resultante",
            "posiciones_estimadas",
            "soporte",
        ]
    ]

    tables = {
        "tabla_capacidad_pasillo": aisle_capacity_base,
        "tabla_demanda_propietario": demand_table,
        "tabla_stock_externo_resumen": external_summary,
        "tabla_stock_externo_equivalencias": stock_external_equivalences,
        "tabla_impacto_stock_externo_propietario": impact_table,
        "tabla_conversiones_infra": conversion_table,
        "tabla_conciliacion_tipologias": reconciliation,
        "tabla_propietario_pasillo_recomendado": pd.concat([assignment_base, assignment_conditioned], ignore_index=True),
        "tabla_articulos_conflictivos": conflict_table,
        "tabla_supuestos": suppositions,
        "tabla_escenarios_ocupacion": scenarios,
        "tabla_validacion_capacidad_destino": capacity_validation,
    }
    support_tables = {
        "tabla_validaciones_obligatorias": validation_table,
        "slot_level_current": slot_level,
        "slot_article_current": slot_article,
        "owner_metrics": owner_metrics,
        "external_equivalences_enriched": external_equivalences,
        "aisle_capacity_conditioned": aisle_capacity_conditioned,
    }
    _save_csv_tables(paths, tables)
    _save_support_tables(paths, support_tables)
    _save_workbook_manifest(paths, tables)

    occupancy_plot_source = (
        slot_level.groupby(["pasillo", "tipologia"], dropna=False)
        .size()
        .reset_index(name="ocupacion")
    )
    _plot_heatmap(occupancy_plot_source, "tipologia", "pasillo", "ocupacion", "Heatmap ocupación por pasillo y tipología", paths.plot_dir / "heatmap_ocupacion_pasillo_tipologia.png")
    owner_passage_plot = (
        slot_level[slot_level["assigned_owner"].notna()]
        .groupby(["assigned_owner", "pasillo"], dropna=False)
        .size()
        .reset_index(name="ocupacion")
        .rename(columns={"assigned_owner": "propietario"})
    )
    owner_passage_plot["propietario"] = owner_passage_plot["propietario"].astype(int).astype(str)
    _plot_heatmap(owner_passage_plot, "propietario", "pasillo", "ocupacion", "Matriz propietario x pasillo", paths.plot_dir / "matriz_propietario_pasillo.png")
    _plot_rankings(demand, owner_names, paths.plot_dir / "ranking_combinado_propietarios.png")
    _plot_waterfall(capacity_validation, demand, paths.plot_dir / "waterfall_capacidad_demanda.png")
    _plot_modules(conversion_table, paths.plot_dir / "modulos_por_tipologia.png")
    _plot_external_impact(impact_table, paths.plot_dir / "impacto_stock_externo_propietario.png")
    _plot_external_coverage(external_equivalences, paths.plot_dir / "cobertura_stock_externo.png")
    _plot_logical_layout(assignment_base, "Layout lógico simplificado escenario base", paths.plot_dir / "layout_logico_base.png")
    _plot_logical_layout(assignment_conditioned, "Layout lógico simplificado escenario condicionado", paths.plot_dir / "layout_logico_condicionado.png")
    _plot_conciliation(reconciliation, paths.plot_dir / "conciliacion_entre_estudios.png")

    _render_summary_markdown(
        paths,
        demand,
        capacity_validation,
        assignment_base,
        assignment_conditioned,
        external_summary,
        validation_table,
        reconciliation,
        conversion_table,
    )

    return {
        "output_dir": paths.output_dir,
        "csv_dir": paths.csv_dir,
        "plot_dir": paths.plot_dir,
        "summary_path": paths.output_dir / "resumen_codex.md",
    }
