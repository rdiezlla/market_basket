from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import math
import re

import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import pandas as pd

from .mahou_dimensioning import (
    DESTINATION_AISLES,
    _ensure_directories,
    _external_owner_assignment,
    _normalize_dataframe_columns,
    _normalize_text,
    _owner_metrics,
    _prepare_owner_map,
    _prepare_movements,
    _prepare_requests,
    _prepare_stock,
)


POSITIONS_PER_DEST_AISLE = 108
SPECIAL_ORDER = ["suelo_300", "suelo_250", "suelo_126", "balda_9h"]
MODULE_SIZE_3EU = 3
DESTINATION_PHYSICAL_SEQUENCE = list(range(1, POSITIONS_PER_DEST_AISLE + 1, 2)) + list(range(2, POSITIONS_PER_DEST_AISLE + 1, 2))
VISUAL_LABELS = {
    "suelo_300": "3.00 m",
    "suelo_250": "2.50 m",
    "suelo_126": "1.26 + palet",
    "balda_9h": "Balda 9h",
    "puente": "Puente",
    "normal": "Normal 1.90",
    "buffer": "Buffer",
}
VISUAL_COLORS = {
    "suelo_300": "#f6ad55",
    "suelo_250": "#f6e05e",
    "suelo_126": "#90cdf4",
    "balda_9h": "#9ae6b4",
    "puente": "#cbd5e0",
    "normal": "#f7fafc",
    "buffer": "#e2e8f0",
}
DISPLAY_ODD_POSITIONS = list(range(1, 55, 2)) + [55, 57, 59] + list(range(61, 115, 2))
DISPLAY_EVEN_POSITIONS = list(range(2, 56, 2)) + [56, 58, 60] + list(range(62, 115, 2))
ABC_COLORS = {
    "A": "#f6ad55",
    "B": "#f6e05e",
    "C": "#9ae6b4",
    "SIN_ABC": "#cbd5e0",
    "PUENTE": "#a0aec0",
    "BUFFER": "#e2e8f0",
}
PICKING_ABC_START = pd.Timestamp("2026-01-01")
PICKING_ABC_CUTOFF = pd.Timestamp("2026-04-20")


@dataclass(frozen=True)
class SourceSpecialRule:
    pasillo: int
    lado: str
    posicion_desde: int
    posicion_hasta: int
    tipologia: str
    nota: str = ""
    excludes: tuple[int, ...] = ()


SOURCE_SPECIAL_RULES: list[SourceSpecialRule] = [
    SourceSpecialRule(7, "todos", 1, 70, "suelo_250"),
    SourceSpecialRule(8, "todos", 1, 84, "suelo_250"),
    SourceSpecialRule(9, "todos", 1, 84, "suelo_250", excludes=(67, 69, 76)),
    SourceSpecialRule(10, "impar", 61, 125, "balda_9h", "subhuecos 01..09"),
    SourceSpecialRule(10, "par", 62, 126, "suelo_250", "texto original 2.40m tratado como 2.50m"),
    SourceSpecialRule(11, "par", 44, 60, "balda_9h", "subhuecos 01..09"),
    SourceSpecialRule(11, "par", 62, 126, "suelo_126"),
    SourceSpecialRule(11, "impar", 61, 125, "suelo_250"),
    SourceSpecialRule(12, "impar", 1, 41, "suelo_126"),
    SourceSpecialRule(12, "impar", 43, 59, "balda_9h", "subhuecos 01..09"),
    SourceSpecialRule(12, "impar", 61, 125, "suelo_126"),
    SourceSpecialRule(12, "par", 2, 18, "suelo_300"),
    SourceSpecialRule(12, "par", 20, 60, "suelo_126"),
    SourceSpecialRule(12, "par", 62, 78, "suelo_300"),
    SourceSpecialRule(13, "impar", 1, 17, "suelo_300"),
    SourceSpecialRule(13, "impar", 19, 59, "suelo_126"),
    SourceSpecialRule(13, "impar", 61, 77, "suelo_300"),
    SourceSpecialRule(13, "par", 2, 6, "suelo_250"),
    SourceSpecialRule(13, "par", 14, 58, "suelo_126"),
    SourceSpecialRule(13, "par", 62, 72, "suelo_250"),
    SourceSpecialRule(14, "impar", 1, 11, "suelo_250"),
    SourceSpecialRule(14, "par", 2, 12, "suelo_250"),
    SourceSpecialRule(14, "todos", 13, 60, "suelo_126"),
    SourceSpecialRule(14, "todos", 61, 72, "suelo_250"),
    SourceSpecialRule(14, "todos", 134, 162, "suelo_250"),
    SourceSpecialRule(15, "impar", 1, 11, "suelo_250"),
    SourceSpecialRule(15, "impar", 13, 59, "suelo_126"),
    SourceSpecialRule(15, "impar", 61, 71, "suelo_250"),
    SourceSpecialRule(15, "impar", 133, 161, "suelo_250"),
    SourceSpecialRule(15, "par", 2, 48, "suelo_126"),
    SourceSpecialRule(15, "par", 50, 78, "suelo_250"),
    SourceSpecialRule(15, "par", 134, 162, "suelo_250"),
    SourceSpecialRule(16, "impar", 1, 47, "suelo_126"),
    SourceSpecialRule(16, "impar", 49, 77, "suelo_250"),
    SourceSpecialRule(18, "par", 2, 126, "suelo_300", "lado impar no existe"),
    SourceSpecialRule(19, "todos", 1, 162, "suelo_300"),
    SourceSpecialRule(20, "impar", 1, 162, "suelo_300"),
    SourceSpecialRule(20, "par", 2, 162, "suelo_250"),
]


def _rule_positions(rule: SourceSpecialRule) -> list[int]:
    positions = []
    start = int(rule.posicion_desde)
    end = int(rule.posicion_hasta)
    for value in range(start, end + 1):
        if rule.lado == "impar" and value % 2 == 0:
            continue
        if rule.lado == "par" and value % 2 == 1:
            continue
        if value in rule.excludes:
            continue
        positions.append(value)
    return positions


def _load_sources(base_dir: Path) -> dict[str, pd.DataFrame]:
    stock = _normalize_dataframe_columns(pd.read_excel(base_dir / "17-04-2026.xlsx"))
    external = _normalize_dataframe_columns(pd.read_excel(base_dir / "STOCK_MAP_FACT_MAHOU_260415020005.xlsx", sheet_name="STOCKS"))
    owner_map = _normalize_dataframe_columns(pd.read_excel(base_dir / "propietario_departamento.xlsx"))
    movements = _normalize_dataframe_columns(pd.read_excel(base_dir / "movimientos.xlsx"))
    requests = _normalize_dataframe_columns(pd.read_excel(base_dir / "lineas_solicitudes_con_pedidos.xlsx"))
    return {
        "stock": stock,
        "external": external,
        "owner_map": owner_map,
        "movements": movements,
        "requests": requests,
    }


def _physical_parts_for_sequence_range(position_from: int, position_to: int) -> list[dict[str, object]]:
    values = DESTINATION_PHYSICAL_SEQUENCE[position_from - 1 : position_to]
    if not values:
        return []
    parts: list[dict[str, object]] = []
    current_side = "impar" if values[0] % 2 == 1 else "par"
    current_values = [values[0]]
    for value in values[1:]:
        side = "impar" if value % 2 == 1 else "par"
        if side == current_side and value - current_values[-1] == 2:
            current_values.append(value)
            continue
        parts.append(
            {
                "lado_destino": current_side,
                "posicion_desde_fisica": current_values[0],
                "posicion_hasta_fisica": current_values[-1],
            }
        )
        current_side = side
        current_values = [value]
    parts.append(
        {
            "lado_destino": current_side,
            "posicion_desde_fisica": current_values[0],
            "posicion_hasta_fisica": current_values[-1],
        }
    )
    return parts


def _physical_range_label(position_from: int, position_to: int) -> tuple[str, str]:
    parts = _physical_parts_for_sequence_range(position_from, position_to)
    if not parts:
        return "", ""
    labels = []
    sides = []
    for part in parts:
        sides.append(str(part["lado_destino"]))
        start = int(part["posicion_desde_fisica"])
        end = int(part["posicion_hasta_fisica"])
        if start == end:
            labels.append(f"{part['lado_destino']}:{start:03d}")
        else:
            labels.append(f"{part['lado_destino']}:{start:03d}-{end:03d}")
    unique_sides = list(dict.fromkeys(sides))
    return " | ".join(unique_sides), " | ".join(labels)


def _append_block(
    rows: list[dict[str, object]],
    *,
    aisle: int,
    position_from: int,
    position_to: int,
    tipology: str,
    owner: int | None,
    owner_name: str,
    block_type: str,
    padding_3eu: int = 0,
) -> None:
    sides, physical_label = _physical_range_label(position_from, position_to)
    rows.append(
        {
            "pasillo_destino": aisle,
            "posicion_desde": position_from,
            "posicion_hasta": position_to,
            "tipologia": tipology,
            "propietario": owner if owner is not None else pd.NA,
            "owner_name": owner_name,
            "tipo_bloque": block_type,
            "padding_modulo_3eu": int(padding_3eu),
            "lado_destino": sides,
            "tramo_fisico": physical_label,
        }
    )


def _source_layout_tables() -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    for rule in SOURCE_SPECIAL_RULES:
        positions = _rule_positions(rule)
        rows.append(
            {
                "pasillo_origen": rule.pasillo,
                "lado": rule.lado,
                "tipologia": rule.tipologia,
                "posicion_desde": rule.posicion_desde,
                "posicion_hasta": rule.posicion_hasta,
                "posiciones_contadas": len(positions),
                "tramo_posiciones": f"{int(rule.posicion_desde):03d}-{int(rule.posicion_hasta):03d}",
                "posiciones_detalle": ",".join(str(value) for value in positions[:30]) + ("..." if len(positions) > 30 else ""),
                "nota": rule.nota or "",
            }
        )
    detail = pd.DataFrame(rows).sort_values(["pasillo_origen", "tipologia", "lado", "posicion_desde"]).reset_index(drop=True)
    summary_rows = []
    for aisle in sorted(detail["pasillo_origen"].unique().tolist()):
        aisle_frame = detail[detail["pasillo_origen"] == aisle].copy()
        row = {"pasillo_origen": aisle}
        for tipology in SPECIAL_ORDER:
            tip_frame = aisle_frame[aisle_frame["tipologia"] == tipology].copy()
            row[f"{tipology}_posiciones"] = int(tip_frame["posiciones_contadas"].sum()) if not tip_frame.empty else 0
            row[f"{tipology}_tramos"] = " | ".join(
                f"{detail_row.lado}:{detail_row.tramo_posiciones}"
                for detail_row in tip_frame.itertuples(index=False)
            )
        summary_rows.append(row)
    summary = pd.DataFrame(summary_rows)
    return detail, summary


def _largest_remainder_allocation(weights: dict[int, float], target_total: int, caps: dict[int, int] | None = None) -> dict[int, int]:
    keys = list(weights.keys())
    if target_total <= 0 or not keys:
        return {key: 0 for key in keys}
    caps = caps or {key: target_total for key in keys}
    positive = {key: max(float(weights.get(key, 0.0)), 0.0) for key in keys if caps.get(key, 0) > 0}
    if not positive:
        return {key: 0 for key in keys}
    total_weight = sum(positive.values())
    if total_weight <= 0:
        ordered = sorted(positive.keys())
        allocation = {key: 0 for key in keys}
        remaining = target_total
        for key in ordered:
            if remaining <= 0:
                break
            take = min(caps.get(key, 0), remaining)
            allocation[key] = take
            remaining -= take
        return allocation

    raw = {key: target_total * positive[key] / total_weight for key in positive}
    allocation = {key: min(int(math.floor(raw[key])), caps.get(key, 0)) for key in positive}
    remaining = target_total - sum(allocation.values())
    remainders = sorted(
        positive.keys(),
        key=lambda key: (raw[key] - math.floor(raw[key]), positive[key], -key),
        reverse=True,
    )
    while remaining > 0:
        moved = False
        for key in remainders:
            if allocation[key] >= caps.get(key, 0):
                continue
            allocation[key] += 1
            remaining -= 1
            moved = True
            if remaining <= 0:
                break
        if not moved:
            break

    return {key: allocation.get(key, 0) for key in keys}


def _source_special_targets(source_summary: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for tipology in SPECIAL_ORDER:
        raw_total = int(pd.to_numeric(source_summary.get(f"{tipology}_posiciones", 0), errors="coerce").fillna(0).sum())
        target_total = int(math.ceil(raw_total / MODULE_SIZE_3EU) * MODULE_SIZE_3EU) if raw_total > 0 else 0
        rows.append(
            {
                "tipologia": tipology,
                "origen_posiciones": raw_total,
                "destino_objetivo_posiciones": target_total,
                "delta_redondeo_modulo_3eu": target_total - raw_total,
            }
        )
    return pd.DataFrame(rows)


def _owner_ranking_table(sources: dict[str, pd.DataFrame]) -> pd.DataFrame:
    movements = _prepare_movements(sources["movements"])
    requests = _prepare_requests(sources["requests"])
    rankings = _owner_metrics(movements, requests)[
        [
            "owner",
            "salidas_lineas",
            "salidas_transacciones",
            "volumen_unidades",
            "servicios_lineas",
            "ranking_operativo_score",
        ]
    ].copy()
    ytd = movements[
        movements["fecha_finalizacion"].between(PICKING_ABC_START, PICKING_ABC_CUTOFF, inclusive="both")
    ].copy()
    ytd_metrics = (
        ytd.groupby("owner", dropna=False)
        .agg(
            salidas_lineas_2026=("transaction_id", "count"),
            salidas_transacciones_2026=("transaction_id", "nunique"),
            volumen_unidades_2026=("cantidad", "sum"),
            ultima_salida_2026=("fecha_finalizacion", "max"),
        )
        .reset_index()
    )
    rankings["owner"] = pd.to_numeric(rankings["owner"], errors="coerce")
    rankings = rankings[rankings["owner"].notna()].copy()
    rankings["owner"] = rankings["owner"].astype(int)
    ytd_metrics["owner"] = pd.to_numeric(ytd_metrics["owner"], errors="coerce")
    ytd_metrics = ytd_metrics[ytd_metrics["owner"].notna()].copy()
    ytd_metrics["owner"] = ytd_metrics["owner"].astype(int)
    rankings = rankings.merge(ytd_metrics, on="owner", how="left")
    for column in ["salidas_lineas_2026", "salidas_transacciones_2026", "volumen_unidades_2026"]:
        rankings[column] = pd.to_numeric(rankings[column], errors="coerce").fillna(0.0)
    return rankings


def _assign_abc(series: pd.Series) -> pd.Series:
    total = float(series.sum())
    if total <= 0:
        return pd.Series(["SIN_ABC"] * len(series), index=series.index)
    cumulative = series.cumsum() / total
    classes = []
    for index, value in enumerate(cumulative):
        if index == 0 or value <= 0.80:
            classes.append("A")
        elif value <= 0.95:
            classes.append("B")
        else:
            classes.append("C")
    return pd.Series(classes, index=series.index)


def _owner_picking_abc_2026(sources: dict[str, pd.DataFrame], owner_names: dict[int, str]) -> pd.DataFrame:
    movements = _prepare_movements(sources["movements"])
    frame = movements[
        movements["fecha_finalizacion"].between(PICKING_ABC_START, PICKING_ABC_CUTOFF, inclusive="both")
    ].copy()

    grouped = (
        frame.groupby("owner", dropna=False)
        .agg(
            picking_lineas_2026=("transaction_id", "count"),
            picking_transacciones_2026=("transaction_id", "nunique"),
            picking_unidades_2026=("cantidad", "sum"),
            articulos_2026=("article", "nunique"),
            ultima_salida_2026=("fecha_finalizacion", "max"),
        )
        .reset_index()
    )
    grouped["owner"] = pd.to_numeric(grouped["owner"], errors="coerce")
    grouped = grouped[grouped["owner"].notna()].copy()
    grouped["owner"] = grouped["owner"].astype(int)
    grouped["owner_name"] = grouped["owner"].map(lambda owner: owner_names.get(int(owner), str(int(owner))))
    grouped = grouped.sort_values(
        ["picking_lineas_2026", "picking_transacciones_2026", "picking_unidades_2026"],
        ascending=[False, False, False],
    ).reset_index(drop=True)
    grouped["share_lineas_2026"] = grouped["picking_lineas_2026"] / grouped["picking_lineas_2026"].sum()
    grouped["share_acumulada_lineas_2026"] = grouped["share_lineas_2026"].cumsum()
    grouped["abc_picking_2026"] = _assign_abc(grouped["picking_lineas_2026"])
    grouped["periodo_inicio"] = PICKING_ABC_START.date().isoformat()
    grouped["periodo_corte"] = PICKING_ABC_CUTOFF.date().isoformat()
    return grouped


def _source_owner_inventory(
    slot_level: pd.DataFrame,
    external_assigned: pd.DataFrame,
    assignment_base: pd.DataFrame,
    owner_names: dict[int, str],
) -> pd.DataFrame:
    photostock_owners = {
        int(owner)
        for owner in pd.to_numeric(slot_level["assigned_owner"], errors="coerce").dropna().astype(int).unique().tolist()
    }
    external_owners = {
        int(owner)
        for owner in pd.to_numeric(external_assigned["owner_base"], errors="coerce").dropna().astype(int).unique().tolist()
        if int(owner) <= 100
    }
    assigned_owners = {
        int(owner)
        for owner in pd.to_numeric(assignment_base["propietario"], errors="coerce").dropna().astype(int).unique().tolist()
    }
    all_owners = sorted(photostock_owners | external_owners)

    rows = []
    for owner in all_owners:
        rows.append(
            {
                "propietario": owner,
                "owner_name": owner_names.get(owner, str(owner)),
                "en_fotostock": owner in photostock_owners,
                "en_stock_externo": owner in external_owners,
                "incluido_layout_previo_codex": owner in assigned_owners,
            }
        )
    return pd.DataFrame(rows)


def _numeric_owner_name_map(owner_map: pd.DataFrame, slot_level: pd.DataFrame) -> dict[int, str]:
    names = {}
    if {"assigned_owner", "denominacion_propietario"}.issubset(slot_level.columns):
        current_names = (
            slot_level[slot_level["assigned_owner"].notna()]
            .assign(owner_num=pd.to_numeric(slot_level["assigned_owner"], errors="coerce"))
            .dropna(subset=["owner_num"])
            .groupby("owner_num")["denominacion_propietario"]
            .agg(lambda series: series.mode().iat[0] if not series.mode().empty else series.iloc[0])
        )
        for owner, name in current_names.items():
            names[int(owner)] = _normalize_text(name)

    if {"propietario_num", "departamento"}.issubset(owner_map.columns):
        owner_rows = owner_map.dropna(subset=["propietario_num"]).copy()
        owner_rows["propietario_num"] = owner_rows["propietario_num"].astype(int)
        for row in owner_rows.itertuples(index=False):
            names.setdefault(int(row.propietario_num), _normalize_text(row.departamento))

    return names


def _load_base_outputs(base_dir: Path) -> dict[str, pd.DataFrame]:
    csv_dir = base_dir / "output" / "mahou_codex" / "csv"
    support_dir = base_dir / "output" / "mahou_codex" / "support"
    return {
        "demanda": pd.read_csv(csv_dir / "tabla_demanda_propietario.csv"),
        "asignacion": pd.read_csv(csv_dir / "tabla_propietario_pasillo_recomendado.csv"),
        "externo_enriched": pd.read_csv(support_dir / "external_equivalences_enriched.csv"),
        "slot_level": pd.read_csv(support_dir / "slot_level_current.csv"),
    }


def _owner_requirements(
    demand: pd.DataFrame,
    external_enriched: pd.DataFrame,
    coverage: pd.DataFrame,
    owner_rankings: pd.DataFrame,
    ranking_mode: str = "combined",
) -> pd.DataFrame:
    demand = demand.copy()
    demand["propietario"] = pd.to_numeric(demand["propietario"], errors="coerce")
    demand = demand[demand["propietario"].notna()].copy()
    demand["propietario"] = demand["propietario"].astype(int)

    external_enriched = external_enriched.copy()
    external_enriched["owner_base"] = pd.to_numeric(external_enriched["owner_base"], errors="coerce")
    external_enriched["posiciones_estimadas"] = pd.to_numeric(external_enriched["posiciones_estimadas"], errors="coerce")
    external_enriched["eu_equivalente"] = pd.to_numeric(external_enriched["eu_equivalente"], errors="coerce")
    external_enriched["impacto_10"] = pd.to_numeric(external_enriched["impacto_10"], errors="coerce")
    supported_special = (
        external_enriched[
            (external_enriched["estado_conversion"] == "soportado")
            & (external_enriched["tipologia_estimacion"].isin(["suelo_250", "suelo_300", "suelo_126", "balda_9h"]))
            & external_enriched["owner_base"].notna()
        ]
        .groupby(["owner_base", "tipologia_estimacion"], dropna=False)["posiciones_estimadas"]
        .sum()
        .unstack(fill_value=0)
        .reset_index()
        .rename(columns={"owner_base": "propietario"})
    )
    supported_penalty = (
        external_enriched[
            (external_enriched["estado_conversion"] == "soportado")
            & external_enriched["owner_base"].notna()
        ]
        .groupby("owner_base", dropna=False)["impacto_10"]
        .sum()
        .reset_index()
        .rename(columns={"owner_base": "propietario", "impacto_10": "penalty_10_soportado"})
    )
    all_special = (
        external_enriched[
            external_enriched["tipologia_estimacion"].isin(["suelo_250", "suelo_300", "suelo_126", "balda_9h"])
            & external_enriched["owner_base"].notna()
        ]
        .groupby(["owner_base", "tipologia_estimacion"], dropna=False)["posiciones_estimadas"]
        .sum()
        .unstack(fill_value=0)
        .reset_index()
        .rename(columns={"owner_base": "propietario"})
        .rename(
            columns={
                "suelo_250": "suelo_250_all",
                "suelo_300": "suelo_300_all",
                "suelo_126": "suelo_126_all",
                "balda_9h": "balda_9h_all",
            }
        )
    )
    all_penalty = (
        external_enriched[external_enriched["owner_base"].notna()]
        .groupby("owner_base", dropna=False)["impacto_10"]
        .sum()
        .reset_index()
        .rename(columns={"owner_base": "propietario", "impacto_10": "penalty_10_all"})
    )

    demand = (
        demand.merge(supported_special, on="propietario", how="left")
        .merge(supported_penalty, on="propietario", how="left")
        .merge(all_special, on="propietario", how="left")
        .merge(all_penalty, on="propietario", how="left")
    )
    for column in [
        "suelo_250",
        "suelo_300",
        "suelo_126",
        "balda_9h",
        "penalty_10_soportado",
        "suelo_250_all",
        "suelo_300_all",
        "suelo_126_all",
        "balda_9h_all",
        "penalty_10_all",
    ]:
        if column not in demand.columns:
            demand[column] = 0.0
        demand[column] = pd.to_numeric(demand[column], errors="coerce").fillna(0.0)

    demand["current_eq"] = (
        demand["demanda_actual_eu"] * 1.0
        + demand["demanda_actual_am"] * 1.5
        + demand["demanda_actual_tr"] * 3.0
        + demand["suelo_estandar_actual"] * 1.0
        + demand["suelo_250_actual"] * 1.0
        + demand["suelo_300_actual"] * 1.0
        + demand["suelo_126_actual"] * 1.0
        + demand["balda_9h_actual"] / 9.0
    )
    demand["supported_eq"] = demand["current_eq"] + demand["externo_soportado_total"]
    demand["suelo_250_total"] = demand["suelo_250_actual"] + demand["suelo_250"]
    demand["suelo_300_total"] = demand["suelo_300_actual"] + demand["suelo_300"]
    demand["suelo_126_total"] = demand["suelo_126_actual"] + demand["suelo_126"]
    demand["balda_9h_total_subhuecos"] = demand["balda_9h_actual"] + demand["balda_9h"]
    demand["balda_9h_total_posiciones_suelo"] = demand["balda_9h_total_subhuecos"].map(lambda value: math.ceil(value / 9.0))
    demand["suelo_250_total_all"] = demand["suelo_250_actual"] + demand["suelo_250_all"]
    demand["suelo_300_total_all"] = demand["suelo_300_actual"] + demand["suelo_300_all"]
    demand["suelo_126_total_all"] = demand["suelo_126_actual"] + demand["suelo_126_all"]
    demand["balda_9h_total_subhuecos_all"] = demand["balda_9h_actual"] + demand["balda_9h_all"]
    demand["balda_9h_total_posiciones_suelo_all"] = demand["balda_9h_total_subhuecos_all"].map(lambda value: math.ceil(value / 9.0))
    demand["special_floor_positions_total"] = (
        demand["suelo_250_total"]
        + demand["suelo_300_total"]
        + demand["suelo_126_total"]
        + demand["balda_9h_total_posiciones_suelo"]
    )
    demand["special_floor_positions_total_all"] = (
        demand["suelo_250_total_all"]
        + demand["suelo_300_total_all"]
        + demand["suelo_126_total_all"]
        + demand["balda_9h_total_posiciones_suelo_all"]
    )
    demand["penalty_10_total"] = demand["suelo_250_total"] + demand["suelo_300_total"] + demand["penalty_10_soportado"]
    demand["penalty_10_total_all"] = demand["suelo_250_total_all"] + demand["suelo_300_total_all"] + demand["penalty_10_all"]
    demand["floor_positions_required_90"] = (
        ((demand["supported_eq"] / 0.90) + demand["penalty_10_total"]) / 5.0
    ).map(math.ceil)
    demand["floor_positions_required_90"] = demand[["floor_positions_required_90", "special_floor_positions_total"]].max(axis=1)
    demand["floor_positions_required_fit"] = (
        (demand["supported_eq"] + demand["penalty_10_total"]) / 5.0
    ).map(math.ceil)
    demand["floor_positions_required_fit"] = demand[["floor_positions_required_fit", "special_floor_positions_total"]].max(axis=1)
    demand["conditioned_extra_eq"] = demand["externo_inferido_total"] + demand["externo_revision_manual"]
    demand["floor_positions_required_layout"] = demand["floor_positions_required_fit"]
    demand["suelo_250_total_layout"] = demand["suelo_250_total"]
    demand["suelo_300_total_layout"] = demand["suelo_300_total"]
    demand["suelo_126_total_layout"] = demand["suelo_126_total"]
    demand["balda_9h_total_posiciones_suelo_layout"] = demand["balda_9h_total_posiciones_suelo"]
    demand["penalty_10_total_layout"] = demand["penalty_10_total"]
    demand["layout_support_status"] = "base_soportada"
    conditioned_mask = (demand["floor_positions_required_fit"] <= 0) & (demand["conditioned_extra_eq"] > 0)
    demand.loc[conditioned_mask, "floor_positions_required_layout"] = demand.loc[conditioned_mask].apply(
        lambda row: max(
            int(math.ceil((row["conditioned_extra_eq"] + row["penalty_10_total_all"]) / 5.0)),
            int(row["special_floor_positions_total_all"]),
        ),
        axis=1,
    )
    demand.loc[conditioned_mask, "suelo_250_total_layout"] = demand.loc[conditioned_mask, "suelo_250_total_all"]
    demand.loc[conditioned_mask, "suelo_300_total_layout"] = demand.loc[conditioned_mask, "suelo_300_total_all"]
    demand.loc[conditioned_mask, "suelo_126_total_layout"] = demand.loc[conditioned_mask, "suelo_126_total_all"]
    demand.loc[conditioned_mask, "balda_9h_total_posiciones_suelo_layout"] = demand.loc[
        conditioned_mask, "balda_9h_total_posiciones_suelo_all"
    ]
    demand.loc[conditioned_mask, "penalty_10_total_layout"] = demand.loc[conditioned_mask, "penalty_10_total_all"]
    demand.loc[conditioned_mask, "layout_support_status"] = "condicionado_externo_inferido"
    demand["buffer_positions_to_90"] = demand["floor_positions_required_90"] - demand["floor_positions_required_fit"]

    coverage_cols = coverage[["propietario", "owner_name"]].copy()
    demand = coverage_cols.merge(demand, on="propietario", how="left")
    demand = demand.merge(owner_rankings.rename(columns={"owner": "propietario"}), on="propietario", how="left")
    numeric_columns = [
        column
        for column in demand.columns
        if column not in {"propietario", "owner_name", "layout_support_status"}
    ]
    for column in numeric_columns:
        demand[column] = pd.to_numeric(demand[column], errors="coerce").fillna(0.0)

    demand["observed_suelo_250"] = demand["suelo_250_actual"].round().astype(int)
    demand["observed_suelo_300"] = demand["suelo_300_actual"].round().astype(int)
    demand["observed_suelo_126"] = demand["suelo_126_actual"].round().astype(int)
    demand["observed_balda_subhuecos"] = demand["balda_9h_actual"].round().astype(int)

    if ranking_mode == "salidas_strict":
        sort_columns = [
            "salidas_lineas",
            "salidas_transacciones",
            "supported_eq",
            "floor_positions_required_layout",
        ]
        ascending = [False, False, False, False]
    elif ranking_mode == "rotacion_2026_strict":
        sort_columns = [
            "salidas_lineas_2026",
            "salidas_transacciones_2026",
            "supported_eq",
            "floor_positions_required_layout",
            "salidas_lineas",
        ]
        ascending = [False, False, False, False, False]
    else:
        sort_columns = [
            "ranking_operativo_score",
            "supported_eq",
            "floor_positions_required_fit",
        ]
        ascending = [False, False, False]
    demand = demand.sort_values(sort_columns, ascending=ascending).reset_index(drop=True)
    demand["ranking_mode"] = ranking_mode
    return demand


def _allocate_owner_floor_positions(requirements: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    aisle_rows = []
    current_aisle = 1
    aisle_used = {aisle: 0 for aisle in DESTINATION_AISLES}
    ranking = 1

    for row in requirements.itertuples(index=False):
        owner = int(row.propietario)
        remaining = int(row.floor_positions_required_layout)
        while remaining > 0 and current_aisle <= DESTINATION_AISLES[-1]:
            available = POSITIONS_PER_DEST_AISLE - aisle_used[current_aisle]
            if available <= 0:
                current_aisle += 1
                continue
            allocated = min(available, remaining)
            rows.append(
                {
                    "propietario": owner,
                    "owner_name": row.owner_name,
                    "ranking_operativo": ranking,
                    "pasillo_destino": current_aisle,
                    "floor_positions_allocated": allocated,
                }
            )
            aisle_used[current_aisle] += allocated
            remaining -= allocated
            if aisle_used[current_aisle] >= POSITIONS_PER_DEST_AISLE:
                current_aisle += 1
        aisle_rows.append(
            {
                "propietario": owner,
                "owner_name": row.owner_name,
                "ranking_operativo": ranking,
                "floor_positions_required_90": int(row.floor_positions_required_90),
                "floor_positions_required_fit": int(row.floor_positions_required_fit),
                "floor_positions_required_layout": int(row.floor_positions_required_layout),
                "layout_support_status": row.layout_support_status,
                "floor_positions_assigned": int(row.floor_positions_required_layout - remaining),
                "floor_positions_overflow": int(remaining),
            }
        )
        ranking += 1

    return pd.DataFrame(rows), pd.DataFrame(aisle_rows)


def _observed_special_minima(requirements: pd.DataFrame, source_targets: pd.DataFrame) -> pd.DataFrame:
    minima = requirements[["propietario", "owner_name", "floor_positions_required_layout", "ranking_operativo_score"]].copy()
    minima["suelo_300_min"] = requirements["observed_suelo_300"].astype(int)
    minima["suelo_250_min"] = requirements["observed_suelo_250"].astype(int)
    minima["suelo_126_min"] = requirements["observed_suelo_126"].astype(int)

    target_lookup = source_targets.set_index("tipologia")["destino_objetivo_posiciones"].to_dict()
    balda_target = int(target_lookup.get("balda_9h", 0))
    balda_weights = {
        int(row.propietario): float(max(row.observed_balda_subhuecos, 0))
        for row in requirements.itertuples(index=False)
        if float(max(row.observed_balda_subhuecos, 0)) > 0
    }
    balda_alloc = _largest_remainder_allocation(balda_weights, balda_target)
    minima["balda_9h_min"] = minima["propietario"].map(lambda owner: balda_alloc.get(int(owner), 0)).astype(int)
    minima["special_min_total"] = minima[["suelo_300_min", "suelo_250_min", "suelo_126_min", "balda_9h_min"]].sum(axis=1)
    return minima


def _aisle_type_targets(
    allocations: pd.DataFrame,
    minima: pd.DataFrame,
    source_targets: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    allocations = allocations.copy()
    allocations["propietario"] = allocations["propietario"].astype(int)
    occupancy = (
        allocations.groupby("pasillo_destino", dropna=False)["floor_positions_allocated"]
        .sum()
        .reindex(DESTINATION_AISLES, fill_value=0)
    )

    minima_lookup = minima.set_index("propietario")
    local_rows = []
    type_columns = {
        "suelo_300": "suelo_300_min",
        "suelo_250": "suelo_250_min",
        "suelo_126": "suelo_126_min",
        "balda_9h": "balda_9h_min",
    }
    for owner, owner_alloc in allocations.groupby("propietario", dropna=False):
        owner_alloc = owner_alloc.sort_values("pasillo_destino").copy()
        local_remaining = {
            tipology: int(minima_lookup.loc[int(owner), column]) if int(owner) in minima_lookup.index else 0
            for tipology, column in type_columns.items()
        }
        local_capacity = {int(row.pasillo_destino): int(row.floor_positions_allocated) for row in owner_alloc.itertuples(index=False)}
        used_by_aisle = {aisle: 0 for aisle in local_capacity}
        for tipology in SPECIAL_ORDER:
            remaining = local_remaining[tipology]
            if remaining <= 0:
                continue
            for row in owner_alloc.itertuples(index=False):
                aisle = int(row.pasillo_destino)
                available = local_capacity[aisle] - used_by_aisle[aisle]
                if available <= 0:
                    continue
                take = min(available, remaining)
                if take <= 0:
                    continue
                local_rows.append(
                    {
                        "pasillo_destino": aisle,
                        "propietario": int(owner),
                        "tipologia": tipology,
                        "mandatory_positions": int(take),
                    }
                )
                used_by_aisle[aisle] += take
                remaining -= take
                if remaining <= 0:
                    break

    minima_local = pd.DataFrame(local_rows)
    if minima_local.empty:
        minima_local = pd.DataFrame(columns=["pasillo_destino", "propietario", "tipologia", "mandatory_positions"])
    minima_aisle = (
        minima_local.groupby(["pasillo_destino", "tipologia"], dropna=False)["mandatory_positions"]
        .sum()
        .reset_index()
    )

    occupancy_modules = {aisle: int(occupancy.get(aisle, 0) // MODULE_SIZE_3EU) for aisle in DESTINATION_AISLES}
    aisle_remaining_modules = occupancy_modules.copy()
    target_lookup = source_targets.set_index("tipologia")["destino_objetivo_posiciones"].to_dict()
    target_modules = {tipology: int(target_lookup.get(tipology, 0) // MODULE_SIZE_3EU) for tipology in SPECIAL_ORDER}

    aisle_type_modules = {aisle: {tipology: 0 for tipology in SPECIAL_ORDER} for aisle in DESTINATION_AISLES}

    for tipology in SPECIAL_ORDER:
        baseline_modules = {}
        for aisle in DESTINATION_AISLES:
            raw_mandatory = 0
            match = minima_aisle[
                (minima_aisle["pasillo_destino"] == aisle) & (minima_aisle["tipologia"] == tipology)
            ]
            if not match.empty:
                raw_mandatory = int(match["mandatory_positions"].sum())
            baseline_modules[aisle] = min(
                aisle_remaining_modules[aisle],
                int(math.ceil(raw_mandatory / MODULE_SIZE_3EU)) if raw_mandatory > 0 else 0,
            )

        allocated_modules = sum(baseline_modules.values())
        extra_modules = max(target_modules[tipology] - allocated_modules, 0)
        extra_caps = {aisle: max(aisle_remaining_modules[aisle] - baseline_modules[aisle], 0) for aisle in DESTINATION_AISLES}
        extra_weights = {
            aisle: float(max(extra_caps[aisle], 0)) * (1.0 + ((len(DESTINATION_AISLES) - aisle) * 0.01))
            for aisle in DESTINATION_AISLES
        }
        extra_alloc = _largest_remainder_allocation(extra_weights, extra_modules, caps=extra_caps)

        for aisle in DESTINATION_AISLES:
            modules = baseline_modules[aisle] + extra_alloc.get(aisle, 0)
            aisle_type_modules[aisle][tipology] = modules
            aisle_remaining_modules[aisle] -= modules

    target_rows = []
    for aisle in DESTINATION_AISLES:
        for tipology in SPECIAL_ORDER:
            target_rows.append(
                {
                    "pasillo_destino": aisle,
                    "tipologia": tipology,
                    "target_positions": aisle_type_modules[aisle][tipology] * MODULE_SIZE_3EU,
                }
            )
    return minima_local, pd.DataFrame(target_rows)


def _largest_remainder_with_caps_series(weights: dict[int, float], target_total: int, caps: dict[int, int]) -> dict[int, int]:
    allocation = {key: 0 for key in weights}
    remaining = target_total
    remaining_caps = caps.copy()
    while remaining > 0 and sum(max(value, 0) for value in remaining_caps.values()) > 0:
        round_alloc = _largest_remainder_allocation(weights, remaining, caps=remaining_caps)
        moved = sum(round_alloc.values())
        if moved <= 0:
            break
        for key, value in round_alloc.items():
            allocation[key] += value
            remaining_caps[key] = max(remaining_caps[key] - value, 0)
            remaining -= value
        if moved == 0:
            break
    return allocation


def _build_destination_layout(
    requirements: pd.DataFrame,
    allocations: pd.DataFrame,
    minima_local: pd.DataFrame,
    aisle_type_targets: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    requirements_lookup = requirements.set_index("propietario")
    allocations = allocations.copy()
    allocations["propietario"] = allocations["propietario"].astype(int)
    allocations["pasillo_destino"] = allocations["pasillo_destino"].astype(int)
    allocations["floor_positions_allocated"] = allocations["floor_positions_allocated"].astype(int)
    minima_local = minima_local.copy()
    if not minima_local.empty:
        minima_local["propietario"] = minima_local["propietario"].astype(int)
        minima_local["pasillo_destino"] = minima_local["pasillo_destino"].astype(int)
        minima_local["mandatory_positions"] = minima_local["mandatory_positions"].astype(int)
    aisle_type_targets = aisle_type_targets.copy()
    aisle_type_targets["pasillo_destino"] = aisle_type_targets["pasillo_destino"].astype(int)
    aisle_type_targets["target_positions"] = aisle_type_targets["target_positions"].astype(int)

    detail_rows = []
    owner_rows = []
    for aisle in DESTINATION_AISLES:
        aisle_alloc = allocations[allocations["pasillo_destino"] == aisle].sort_values("ranking_operativo").copy()
        if aisle_alloc.empty:
            continue
        position_cursor = 1
        allocation_remaining = {
            int(row.propietario): int(row.floor_positions_allocated)
            for row in aisle_alloc.itertuples(index=False)
        }
        mandatory_lookup = {}
        if not minima_local.empty:
            aisle_minima = minima_local[minima_local["pasillo_destino"] == aisle].copy()
            mandatory_lookup = {
                (int(row.propietario), str(row.tipologia)): int(row.mandatory_positions)
                for row in aisle_minima.itertuples(index=False)
            }

        target_lookup = {
            str(row.tipologia): int(row.target_positions)
            for row in aisle_type_targets[aisle_type_targets["pasillo_destino"] == aisle].itertuples(index=False)
        }

        for tipology in SPECIAL_ORDER:
            type_target = int(target_lookup.get(tipology, 0))
            if type_target <= 0:
                continue
            special_segments = []
            mandatory_taken = 0
            for row in aisle_alloc.itertuples(index=False):
                owner = int(row.propietario)
                available = allocation_remaining[owner]
                needed = int(mandatory_lookup.get((owner, tipology), 0))
                take = min(available, needed, max(type_target - mandatory_taken, 0))
                if take <= 0:
                    continue
                special_segments.append({"propietario": owner, "owner_name": row.owner_name, "take": int(take)})
                mandatory_taken += int(take)
                allocation_remaining[owner] -= int(take)

            extra_needed = max(type_target - mandatory_taken, 0)
            if extra_needed > 0:
                extra_caps = {int(row.propietario): allocation_remaining[int(row.propietario)] for row in aisle_alloc.itertuples(index=False)}
                extra_weights = {
                    int(row.propietario): float(max(allocation_remaining[int(row.propietario)], 0))
                    * (1.0 + float(requirements_lookup.loc[int(row.propietario), "ranking_operativo_score"]) if int(row.propietario) in requirements_lookup.index else 1.0)
                    for row in aisle_alloc.itertuples(index=False)
                }
                extra_alloc = _largest_remainder_allocation(extra_weights, extra_needed, caps=extra_caps)
                for row in aisle_alloc.itertuples(index=False):
                    owner = int(row.propietario)
                    take = int(extra_alloc.get(owner, 0))
                    if take <= 0:
                        continue
                    special_segments.append({"propietario": owner, "owner_name": row.owner_name, "take": take})
                    allocation_remaining[owner] -= take

            for segment in special_segments:
                owner = int(segment["propietario"])
                take = int(segment["take"])
                _append_block(
                    detail_rows,
                    aisle=aisle,
                    position_from=position_cursor,
                    position_to=position_cursor + take - 1,
                    tipology=tipology,
                    owner=owner,
                    owner_name=str(segment["owner_name"]),
                    block_type="especial",
                )
                _append_block(
                    owner_rows,
                    aisle=aisle,
                    position_from=position_cursor,
                    position_to=position_cursor + take - 1,
                    tipology=tipology,
                    owner=owner,
                    owner_name=str(segment["owner_name"]),
                    block_type="especial",
                )
                position_cursor += take

        for row in aisle_alloc.itertuples(index=False):
            owner = int(row.propietario)
            take = allocation_remaining[owner]
            if take <= 0:
                continue
            _append_block(
                detail_rows,
                aisle=aisle,
                position_from=position_cursor,
                position_to=position_cursor + take - 1,
                tipology="normal",
                owner=owner,
                owner_name=row.owner_name,
                block_type="normal",
            )
            _append_block(
                owner_rows,
                aisle=aisle,
                position_from=position_cursor,
                position_to=position_cursor + take - 1,
                tipology="normal",
                owner=owner,
                owner_name=row.owner_name,
                block_type="normal",
            )
            position_cursor += take
            allocation_remaining[owner] = 0

        if position_cursor <= POSITIONS_PER_DEST_AISLE:
            _append_block(
                detail_rows,
                aisle=aisle,
                position_from=position_cursor,
                position_to=POSITIONS_PER_DEST_AISLE,
                tipology="buffer",
                owner=None,
                owner_name="BUFFER",
                block_type="buffer",
            )

    layout_detail = pd.DataFrame(detail_rows)
    owner_ranges = pd.DataFrame(owner_rows)
    return layout_detail, owner_ranges


def _destination_special_summary(layout_detail: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for aisle in DESTINATION_AISLES:
        aisle_frame = layout_detail[layout_detail["pasillo_destino"] == aisle].copy()
        row = {"pasillo_destino": aisle}
        for tipology in SPECIAL_ORDER:
            tip_frame = aisle_frame[aisle_frame["tipologia"] == tipology].copy()
            if tip_frame.empty:
                row[f"{tipology}_posiciones"] = 0
                row[f"{tipology}_tramos"] = ""
                continue
            tip_frame["posiciones"] = tip_frame["posicion_hasta"] - tip_frame["posicion_desde"] + 1
            row[f"{tipology}_posiciones"] = int(tip_frame["posiciones"].sum())
            row[f"{tipology}_tramos"] = " | ".join(
                f"{detail_row.tramo_fisico} ({int(detail_row.propietario)} {detail_row.owner_name}{';pad=' + str(int(detail_row.padding_modulo_3eu)) if int(detail_row.padding_modulo_3eu) else ''})"
                for detail_row in tip_frame.itertuples(index=False)
            )
        rows.append(row)
    return pd.DataFrame(rows)


def _coverage_with_layout_result(coverage: pd.DataFrame, overflow: pd.DataFrame) -> pd.DataFrame:
    result = coverage.copy()
    overflow = overflow.copy()
    overflow["propietario"] = pd.to_numeric(overflow["propietario"], errors="coerce")
    overflow = overflow[overflow["propietario"].notna()].copy()
    overflow["propietario"] = overflow["propietario"].astype(int)
    result = result.merge(
        overflow[
            [
                "propietario",
                "floor_positions_required_90",
                "floor_positions_required_fit",
                "floor_positions_required_layout",
                "layout_support_status",
                "floor_positions_assigned",
                "floor_positions_overflow",
            ]
        ],
        on="propietario",
        how="left",
    )
    for column in [
        "floor_positions_required_90",
        "floor_positions_required_fit",
        "floor_positions_required_layout",
        "floor_positions_assigned",
        "floor_positions_overflow",
    ]:
        result[column] = pd.to_numeric(result[column], errors="coerce").fillna(0).astype(int)
    result["layout_support_status"] = result["layout_support_status"].fillna("sin_demanda")
    result["incluido_layout_detallado"] = result["floor_positions_assigned"] > 0
    result["estado_cobertura"] = "asignado"
    result.loc[result["floor_positions_required_fit"] == 0, "estado_cobertura"] = "sin_demanda"
    result.loc[result["floor_positions_overflow"] > 0, "estado_cobertura"] = "overflow"
    result.loc[
        result["layout_support_status"] == "condicionado_externo_inferido",
        "estado_cobertura",
    ] = "asignado_condicionado"
    result.loc[
        (result["floor_positions_required_layout"] > 0) & (~result["incluido_layout_detallado"]),
        "estado_cobertura",
    ] = "sin_asignacion"
    return result.sort_values(["incluido_layout_detallado", "propietario"], ascending=[False, True]).reset_index(drop=True)


def _owner_ranges_summary(owner_ranges: pd.DataFrame) -> pd.DataFrame:
    if owner_ranges.empty:
        return pd.DataFrame(
            columns=[
                "pasillo_destino",
                "propietario",
                "owner_name",
                "rangos_suelo",
                "rangos_fisicos",
                "posicion_desde_global",
                "posicion_hasta_global",
                "posiciones_totales",
            ]
        )

    export = owner_ranges.copy()
    export["posiciones_totales"] = export["posicion_hasta"] - export["posicion_desde"] + 1
    summary = (
        export.sort_values(["pasillo_destino", "propietario", "posicion_desde"])
        .groupby(["pasillo_destino", "propietario", "owner_name"], dropna=False)
        .agg(
            rangos_suelo=(
                "posicion_desde",
                lambda series: " | ".join(
                    f"{int(start):03d}-{int(end):03d} ({tip}{';pad=' + str(int(pad)) if pad else ''})"
                    for start, end, tip, pad in zip(
                        series.tolist(),
                        export.loc[series.index, "posicion_hasta"].tolist(),
                        export.loc[series.index, "tipologia"].tolist(),
                        export.loc[series.index, "padding_modulo_3eu"].tolist(),
                    )
                ),
            ),
            rangos_fisicos=("tramo_fisico", lambda series: " | ".join(series.astype(str).tolist())),
            posicion_desde_global=("posicion_desde", "min"),
            posicion_hasta_global=("posicion_hasta", "max"),
            posiciones_totales=("posiciones_totales", "sum"),
        )
        .reset_index()
    )
    return summary


def _expand_height00_positions(layout_detail: pd.DataFrame) -> pd.DataFrame:
    def _display_position(physical_position: int) -> int:
        return int(physical_position if physical_position <= 54 else physical_position + 6)

    rows = []
    for row in layout_detail.itertuples(index=False):
        physical_positions = DESTINATION_PHYSICAL_SEQUENCE[int(row.posicion_desde) - 1 : int(row.posicion_hasta)]
        for physical_position in physical_positions:
            display_position = _display_position(int(physical_position))
            rows.append(
                {
                    "pasillo_destino": int(row.pasillo_destino),
                    "lado_destino": "impar" if int(physical_position) % 2 == 1 else "par",
                    "posicion_fisica": int(physical_position),
                    "posicion_visual": display_position,
                    "ubicacion_destino": f"{int(row.pasillo_destino):03d}-{display_position:03d}-00",
                    "tipologia": str(row.tipologia),
                    "etiqueta_visual": VISUAL_LABELS.get(str(row.tipologia), str(row.tipologia)),
                    "propietario": row.propietario if pd.notna(row.propietario) else pd.NA,
                    "owner_name": row.owner_name,
                    "tipo_bloque": row.tipo_bloque,
                }
            )
    for aisle in DESTINATION_AISLES:
        for side, bridge_positions in [("impar", [55, 57, 59]), ("par", [56, 58, 60])]:
            for display_position in bridge_positions:
                rows.append(
                    {
                        "pasillo_destino": int(aisle),
                        "lado_destino": side,
                        "posicion_fisica": pd.NA,
                        "posicion_visual": int(display_position),
                        "ubicacion_destino": f"{int(aisle):03d}-{int(display_position):03d}-00",
                        "tipologia": "puente",
                        "etiqueta_visual": VISUAL_LABELS["puente"],
                        "propietario": pd.NA,
                        "owner_name": "PUENTE",
                        "tipo_bloque": "puente",
                    }
                )
    return (
        pd.DataFrame(rows)
        .sort_values(["pasillo_destino", "lado_destino", "posicion_visual"])
        .reset_index(drop=True)
    )


def _draw_altura00_aisle(ax: plt.Axes, aisle_positions: pd.DataFrame, aisle: int) -> None:
    odd_positions = list(reversed(DISPLAY_ODD_POSITIONS))
    even_positions = list(reversed(DISPLAY_EVEN_POSITIONS))
    row_count = len(odd_positions)
    col_widths = {"odd_pos": 0.9, "odd_type": 1.35, "even_type": 1.35, "even_pos": 0.9}
    x_positions = {
        "odd_pos": 0.0,
        "odd_type": 0.9,
        "even_type": 2.25,
        "even_pos": 3.60,
    }
    total_width = 4.50
    header_height = 1.25

    ax.set_xlim(0, total_width)
    ax.set_ylim(header_height + row_count, 0)
    ax.axis("off")

    header_specs = [
        ("odd_pos", "Impar"),
        ("odd_type", f"Pasillo {aisle}"),
        ("even_type", f"Pasillo {aisle}"),
        ("even_pos", "Par"),
    ]
    for column, title in header_specs:
        ax.add_patch(
            Rectangle(
                (x_positions[column], 0),
                col_widths[column],
                header_height,
                facecolor="#edf2f7",
                edgecolor="#1a202c",
                linewidth=1.0,
            )
        )
        ax.text(
            x_positions[column] + col_widths[column] / 2.0,
            header_height / 2.0,
            title,
            ha="center",
            va="center",
            fontsize=8,
            fontweight="bold",
        )

    def _type_map(side: str, values: list[int]) -> list[str]:
        mapping = (
            aisle_positions[aisle_positions["lado_destino"] == side]
            .set_index("posicion_visual")["tipologia"]
            .to_dict()
        )
        return [str(mapping.get(value, "buffer")) for value in values]

    odd_types = _type_map("impar", odd_positions)
    even_types = _type_map("par", even_positions)

    for index, (odd_position, even_position) in enumerate(zip(odd_positions, even_positions), start=0):
        y = header_height + index
        for column, position in [("odd_pos", odd_position), ("even_pos", even_position)]:
            ax.add_patch(
                Rectangle(
                    (x_positions[column], y),
                    col_widths[column],
                    1.0,
                    facecolor="#ffffff",
                    edgecolor="#1a202c",
                    linewidth=0.8,
                )
            )
            ax.text(
                x_positions[column] + col_widths[column] / 2.0,
                y + 0.5,
                f"{position:03d}",
                ha="center",
                va="center",
                fontsize=6.5,
            )

    def _draw_type_runs(side_column: str, side_types: list[str]) -> None:
        start_index = 0
        while start_index < len(side_types):
            tipology = side_types[start_index]
            end_index = start_index
            while end_index + 1 < len(side_types) and side_types[end_index + 1] == tipology:
                end_index += 1
            y = header_height + start_index
            height = end_index - start_index + 1
            ax.add_patch(
                Rectangle(
                    (x_positions[side_column], y),
                    col_widths[side_column],
                    height,
                    facecolor=VISUAL_COLORS.get(tipology, "#ffffff"),
                    edgecolor="#1a202c",
                    linewidth=0.8,
                )
            )
            label = VISUAL_LABELS.get(tipology, tipology)
            if tipology != "normal" or height >= 5:
                ax.text(
                    x_positions[side_column] + col_widths[side_column] / 2.0,
                    y + height / 2.0,
                    label,
                    ha="center",
                    va="center",
                    fontsize=7,
                    fontweight="bold" if tipology != "normal" else "normal",
                    wrap=True,
                )
            start_index = end_index + 1

    _draw_type_runs("odd_type", odd_types)
    _draw_type_runs("even_type", even_types)


def _plot_altura00_visuals(height00_positions: pd.DataFrame, plot_dir: Path) -> dict[str, Path]:
    plot_dir.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, Path] = {}

    for aisle in DESTINATION_AISLES:
        aisle_frame = height00_positions[height00_positions["pasillo_destino"] == aisle].copy()
        if aisle_frame.empty:
            continue
        fig, ax = plt.subplots(figsize=(5.2, 14.5))
        _draw_altura00_aisle(ax, aisle_frame, aisle)
        fig.tight_layout()
        output_path = plot_dir / f"altura00_pasillo_{aisle:02d}.png"
        fig.savefig(output_path, dpi=200, bbox_inches="tight")
        plt.close(fig)
        outputs[f"aisle_{aisle:02d}"] = output_path

    fig, axes = plt.subplots(3, 4, figsize=(18, 32))
    for axis, aisle in zip(axes.flatten(), DESTINATION_AISLES):
        aisle_frame = height00_positions[height00_positions["pasillo_destino"] == aisle].copy()
        if aisle_frame.empty:
            axis.axis("off")
            continue
        _draw_altura00_aisle(axis, aisle_frame, aisle)
    fig.tight_layout()
    contact_path = plot_dir / "altura00_destino_contact_sheet.png"
    fig.savefig(contact_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    outputs["contact_sheet"] = contact_path
    return outputs


def _short_owner_visual(owner: object, owner_name: object) -> str:
    owner_label = "" if owner is None or pd.isna(owner) else str(int(float(owner)))
    name = _normalize_text(owner_name)
    if not name:
        return owner_label or "SIN_OWNER"
    compact = name[:18].strip()
    return f"{owner_label} {compact}".strip()


def _owner_visual_positions(
    allocations: pd.DataFrame,
    owner_abc_2026: pd.DataFrame,
    owner_names: dict[int, str],
) -> pd.DataFrame:
    def _display_position(physical_position: int) -> int:
        return int(physical_position if physical_position <= 54 else physical_position + 6)

    owner_abc = owner_abc_2026.rename(columns={"owner": "propietario"}).copy()
    owner_abc["propietario"] = pd.to_numeric(owner_abc["propietario"], errors="coerce")
    allocations = allocations.copy()
    allocations["propietario"] = pd.to_numeric(allocations["propietario"], errors="coerce")
    allocations["pasillo_destino"] = pd.to_numeric(allocations["pasillo_destino"], errors="coerce")
    allocations["ranking_operativo"] = pd.to_numeric(allocations["ranking_operativo"], errors="coerce")
    allocations["floor_positions_allocated"] = pd.to_numeric(allocations["floor_positions_allocated"], errors="coerce")

    rows: list[dict[str, object]] = []
    for aisle in DESTINATION_AISLES:
        aisle_alloc = allocations[allocations["pasillo_destino"] == aisle].sort_values(
            ["ranking_operativo", "propietario"], ascending=[True, True]
        )
        sequence_cursor = 1
        for row in aisle_alloc.itertuples(index=False):
            owner = int(row.propietario)
            take = int(row.floor_positions_allocated)
            physical_positions = DESTINATION_PHYSICAL_SEQUENCE[sequence_cursor - 1 : sequence_cursor - 1 + take]
            for physical_position in physical_positions:
                display_position = _display_position(int(physical_position))
                rows.append(
                    {
                        "pasillo_destino": int(aisle),
                        "lado_destino": "impar" if int(physical_position) % 2 == 1 else "par",
                        "posicion_fisica": int(physical_position),
                        "posicion_visual": display_position,
                        "ubicacion_destino": f"{int(aisle):03d}-{display_position:03d}-00",
                        "tipologia": "owner_block",
                        "propietario": owner,
                        "owner_name": owner_names.get(owner, str(owner)),
                    }
                )
            sequence_cursor += take

        if sequence_cursor <= POSITIONS_PER_DEST_AISLE:
            for physical_position in DESTINATION_PHYSICAL_SEQUENCE[sequence_cursor - 1 :]:
                display_position = _display_position(int(physical_position))
                rows.append(
                    {
                        "pasillo_destino": int(aisle),
                        "lado_destino": "impar" if int(physical_position) % 2 == 1 else "par",
                        "posicion_fisica": int(physical_position),
                        "posicion_visual": display_position,
                        "ubicacion_destino": f"{int(aisle):03d}-{display_position:03d}-00",
                        "tipologia": "buffer",
                        "propietario": pd.NA,
                        "owner_name": "BUFFER",
                    }
                )

        for side, bridge_positions in [("impar", [55, 57, 59]), ("par", [56, 58, 60])]:
            for display_position in bridge_positions:
                rows.append(
                    {
                        "pasillo_destino": int(aisle),
                        "lado_destino": side,
                        "posicion_fisica": pd.NA,
                        "posicion_visual": int(display_position),
                        "ubicacion_destino": f"{int(aisle):03d}-{int(display_position):03d}-00",
                        "tipologia": "puente",
                        "propietario": pd.NA,
                        "owner_name": "PUENTE",
                    }
                )

    frame = pd.DataFrame(rows).sort_values(["pasillo_destino", "lado_destino", "posicion_visual"]).reset_index(drop=True)
    frame = frame.merge(
        owner_abc[
            [
                "propietario",
                "owner_name",
                "picking_lineas_2026",
                "picking_transacciones_2026",
                "picking_unidades_2026",
                "abc_picking_2026",
            ]
        ],
        on="propietario",
        how="left",
        suffixes=("", "_abc"),
    )
    frame["owner_name_final"] = frame["owner_name_abc"].fillna(frame["owner_name"])
    frame["abc_visual"] = frame["abc_picking_2026"].fillna("SIN_ABC")
    frame.loc[frame["tipologia"] == "puente", "abc_visual"] = "PUENTE"
    frame.loc[frame["tipologia"] == "buffer", "abc_visual"] = "BUFFER"
    frame["label_owner_abc"] = frame.apply(
        lambda row: "Puente"
        if row["tipologia"] == "puente"
        else ("Buffer" if row["tipologia"] == "buffer" else _short_owner_visual(row["propietario"], row["owner_name_final"])),
        axis=1,
    )
    return frame


def _draw_owner_abc_aisle(ax: plt.Axes, aisle_positions: pd.DataFrame, aisle: int) -> None:
    odd_positions = list(reversed(DISPLAY_ODD_POSITIONS))
    even_positions = list(reversed(DISPLAY_EVEN_POSITIONS))
    row_count = len(odd_positions)
    col_widths = {"odd_pos": 0.9, "odd_type": 1.6, "even_type": 1.6, "even_pos": 0.9}
    x_positions = {"odd_pos": 0.0, "odd_type": 0.9, "even_type": 2.5, "even_pos": 4.1}
    total_width = 5.0
    header_height = 1.25

    ax.set_xlim(0, total_width)
    ax.set_ylim(header_height + row_count, 0)
    ax.axis("off")

    for column, title in [("odd_pos", "Impar"), ("odd_type", f"Pasillo {aisle}"), ("even_type", f"Pasillo {aisle}"), ("even_pos", "Par")]:
        ax.add_patch(
            Rectangle((x_positions[column], 0), col_widths[column], header_height, facecolor="#edf2f7", edgecolor="#1a202c", linewidth=1.0)
        )
        ax.text(x_positions[column] + col_widths[column] / 2.0, header_height / 2.0, title, ha="center", va="center", fontsize=8, fontweight="bold")

    def _mapping(side: str) -> dict[int, tuple[str, str, object, float]]:
        side_frame = aisle_positions[aisle_positions["lado_destino"] == side].copy()
        return {
            int(row.posicion_visual): (
                str(row.label_owner_abc),
                str(row.abc_visual),
                row.propietario,
                float(row.picking_lineas_2026) if pd.notna(row.picking_lineas_2026) else 0.0,
            )
            for row in side_frame.itertuples(index=False)
        }

    odd_map = _mapping("impar")
    even_map = _mapping("par")

    for index, (odd_position, even_position) in enumerate(zip(odd_positions, even_positions), start=0):
        y = header_height + index
        for column, position in [("odd_pos", odd_position), ("even_pos", even_position)]:
            ax.add_patch(Rectangle((x_positions[column], y), col_widths[column], 1.0, facecolor="#ffffff", edgecolor="#1a202c", linewidth=0.8))
            ax.text(x_positions[column] + col_widths[column] / 2.0, y + 0.5, f"{position:03d}", ha="center", va="center", fontsize=6.5)

    def _draw_runs(side_column: str, positions: list[int], mapping: dict[int, tuple[str, str, object, float]]) -> None:
        start_index = 0
        while start_index < len(positions):
            current = mapping.get(positions[start_index], ("Buffer", "BUFFER", None, 0.0))
            end_index = start_index
            while end_index + 1 < len(positions) and mapping.get(positions[end_index + 1], ("Buffer", "BUFFER", None, 0.0))[:3] == current[:3]:
                end_index += 1
            y = header_height + start_index
            height = end_index - start_index + 1
            ax.add_patch(
                Rectangle(
                    (x_positions[side_column], y),
                    col_widths[side_column],
                    height,
                    facecolor=ABC_COLORS.get(current[1], "#edf2f7"),
                    edgecolor="#1a202c",
                    linewidth=0.8,
                )
            )
            if height >= 3 or current[1] in {"PUENTE", "BUFFER"}:
                suffix = ""
                if current[1] not in {"PUENTE", "BUFFER"}:
                    suffix = f"\n{current[1]} | {int(current[3])} picks"
                ax.text(
                    x_positions[side_column] + col_widths[side_column] / 2.0,
                    y + height / 2.0,
                    f"{current[0]}{suffix}",
                    ha="center",
                    va="center",
                    fontsize=6.4,
                    fontweight="bold" if current[1] in {"A", "PUENTE"} else "normal",
                    wrap=True,
                )
            start_index = end_index + 1

    _draw_runs("odd_type", odd_positions, odd_map)
    _draw_runs("even_type", even_positions, even_map)


def _plot_owner_abc_visuals(owner_positions: pd.DataFrame, plot_dir: Path) -> dict[str, Path]:
    plot_dir.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, Path] = {}

    for aisle in DESTINATION_AISLES:
        aisle_frame = owner_positions[owner_positions["pasillo_destino"] == aisle].copy()
        if aisle_frame.empty:
            continue
        fig, ax = plt.subplots(figsize=(5.8, 14.5))
        _draw_owner_abc_aisle(ax, aisle_frame, aisle)
        fig.tight_layout()
        output_path = plot_dir / f"propietarios_abc_pasillo_{aisle:02d}.png"
        fig.savefig(output_path, dpi=200, bbox_inches="tight")
        plt.close(fig)
        outputs[f"aisle_{aisle:02d}"] = output_path

    fig, axes = plt.subplots(3, 4, figsize=(19, 32))
    for axis, aisle in zip(axes.flatten(), DESTINATION_AISLES):
        aisle_frame = owner_positions[owner_positions["pasillo_destino"] == aisle].copy()
        if aisle_frame.empty:
            axis.axis("off")
            continue
        _draw_owner_abc_aisle(axis, aisle_frame, aisle)
    fig.tight_layout()
    contact_path = plot_dir / "propietarios_abc_contact_sheet.png"
    fig.savefig(contact_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    outputs["contact_sheet"] = contact_path
    return outputs


def _json_safe_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    export = frame.copy()
    for column in export.columns:
        if pd.api.types.is_datetime64_any_dtype(export[column]):
            export[column] = export[column].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
    return json.loads(export.to_json(orient="records", force_ascii=False, date_format="iso"))


def _render_markdown(
    paths: Path,
    coverage: pd.DataFrame,
    source_summary: pd.DataFrame,
    destination_summary: pd.DataFrame,
    overflow: pd.DataFrame,
    visual_outputs: dict[str, Path] | None = None,
    owner_visual_outputs: dict[str, Path] | None = None,
) -> None:
    missing = coverage[
        (coverage["en_fotostock"] | coverage["en_stock_externo"])
        & (~coverage["incluido_layout_detallado"])
    ].copy()
    lines = [
        "# Layout detallado para entendimiento operativo",
        "",
        "## Cobertura de propietarios",
        f"- Propietarios en fotostock: {int(coverage['en_fotostock'].sum())}",
        f"- Propietarios en stock externo con owner asignado: {int(coverage['en_stock_externo'].sum())}",
        f"- Propietarios incluidos en layout detallado final: {int(coverage['incluido_layout_detallado'].sum())}",
        "",
        "## Pasillos origen con ubicaciones especiales",
    ]
    for row in source_summary.itertuples(index=False):
        parts = []
        for tipology in ["suelo_300", "suelo_250", "suelo_126", "balda_9h"]:
            value = getattr(row, f"{tipology}_posiciones", 0) if hasattr(row, f"{tipology}_posiciones") else 0
            tramo = getattr(row, f"{tipology}_tramos", "") if hasattr(row, f"{tipology}_tramos") else ""
            if value:
                parts.append(f"{tipology}={int(value)} [{tramo}]")
        lines.append(f"- Pasillo {int(row.pasillo_origen)}: " + (", ".join(parts) if parts else "sin especiales"))
    lines.extend(["", "## Pasillos destino propuestos", ""])
    for row in destination_summary.itertuples(index=False):
        parts = []
        for tipology in ["suelo_300", "suelo_250", "suelo_126", "balda_9h"]:
            value = getattr(row, f"{tipology}_posiciones", 0) if hasattr(row, f"{tipology}_posiciones") else 0
            tramo = getattr(row, f"{tipology}_tramos", "") if hasattr(row, f"{tipology}_tramos") else ""
            if value:
                parts.append(f"{tipology}={int(value)} [{tramo}]")
        lines.append(f"- Pasillo {int(row.pasillo_destino)}: " + (", ".join(parts) if parts else "sin especiales"))
    lines.extend(["", "## Propietarios sin hueco suficiente en layout base", ""])
    missing_assigned = overflow[overflow["floor_positions_overflow"] > 0]
    if missing.empty and missing_assigned.empty:
        lines.append("- Todos los propietarios con stock en fotostock o externo asignado aparecen en la propuesta detallada.")
    else:
        for row in missing.itertuples(index=False):
            lines.append(f"- Propietario {row.propietario} {row.owner_name}: aparece en fuente pero no entra en layout detallado.")
        for row in missing_assigned.itertuples(index=False):
            lines.append(
                f"- Propietario {row.propietario} {row.owner_name}: overflow de {row.floor_positions_overflow} posiciones de suelo."
            )
    if visual_outputs:
        lines.extend(
            [
                "",
                "## Visual altura 00",
                f"- Contact sheet: {visual_outputs.get('contact_sheet', '')}",
                "- Se ha generado un PNG por pasillo con columnas impar/par y bloque visual por tipologia en altura 00.",
            ]
        )
    if owner_visual_outputs:
        lines.extend(
            [
                "",
                "## Visual propietarios y ABC 2026",
                f"- Contact sheet propietarios: {owner_visual_outputs.get('contact_sheet', '')}",
                f"- Periodo ABC picking: {PICKING_ABC_START.date().isoformat()} a {PICKING_ABC_CUTOFF.date().isoformat()}",
                "- Colores: A naranja, B amarillo, C verde, sin ABC gris, puente gris oscuro.",
            ]
        )
    (paths / "explicacion_layout_detallado.md").write_text("\n".join(lines), encoding="utf-8")


def run_mahou_layout_detail(
    base_dir: Path,
    *,
    detail_output_root: str = "mahou_codex",
    ranking_mode: str = "combined",
) -> dict[str, Path]:
    paths = _ensure_directories(base_dir)
    detail_root = base_dir / "output" / detail_output_root
    detail_dir = detail_root / "detail"
    detail_dir.mkdir(parents=True, exist_ok=True)

    sources = _load_sources(base_dir)
    owner_map = _prepare_owner_map(sources["owner_map"])
    _, _, slot_level, _ = _prepare_stock(sources["stock"])
    owner_names = _numeric_owner_name_map(owner_map, slot_level)
    external_assigned = _external_owner_assignment(sources["external"], owner_map)
    owner_rankings = _owner_ranking_table(sources)
    owner_abc_2026 = _owner_picking_abc_2026(sources, owner_names)

    outputs = _load_base_outputs(base_dir)
    demand = outputs["demanda"]
    assignment_base = outputs["asignacion"][outputs["asignacion"]["escenario"] == "base"].copy()
    external_enriched = outputs["externo_enriched"]

    source_detail, source_summary = _source_layout_tables()
    source_targets = _source_special_targets(source_summary)
    coverage = _source_owner_inventory(slot_level, external_assigned, assignment_base, owner_names)
    requirements = _owner_requirements(demand, external_enriched, coverage, owner_rankings, ranking_mode=ranking_mode)
    allocations, overflow = _allocate_owner_floor_positions(requirements)
    observed_minima = _observed_special_minima(requirements, source_targets)
    minima_local, aisle_type_targets = _aisle_type_targets(allocations, observed_minima, source_targets)
    coverage_final = _coverage_with_layout_result(coverage, overflow)
    destination_detail, destination_owner_ranges = _build_destination_layout(
        requirements,
        allocations,
        minima_local,
        aisle_type_targets,
    )
    destination_summary = _destination_special_summary(destination_detail)
    destination_owner_ranges_summary = _owner_ranges_summary(destination_owner_ranges)
    height00_positions = _expand_height00_positions(destination_detail)
    visual_plot_dir = detail_dir / "altura00_visual"
    visual_outputs = _plot_altura00_visuals(height00_positions, visual_plot_dir)
    owner_positions = _owner_visual_positions(allocations, owner_abc_2026, owner_names)
    owner_visual_dir = detail_dir / "propietarios_abc_visual"
    owner_visual_outputs = _plot_owner_abc_visuals(owner_positions, owner_visual_dir)

    tables = {
        "tabla_origen_tramos_especiales": source_detail,
        "tabla_origen_resumen_especiales_pasillo": source_summary,
        "tabla_objetivo_tipologias_destino": source_targets,
        "tabla_owner_picking_abc_2026": owner_abc_2026,
        "tabla_cobertura_propietarios": coverage_final,
        "tabla_requerimiento_propietario_layout": requirements,
        "tabla_minimos_observados_propietario": observed_minima,
        "tabla_minimos_observados_pasillo_tipologia": minima_local,
        "tabla_objetivo_pasillo_tipologia": aisle_type_targets,
        "tabla_destino_asignacion_posiciones": allocations,
        "tabla_destino_resumen_especiales_pasillo": destination_summary,
        "tabla_destino_tramos_pasillo": destination_detail,
        "tabla_destino_propietario_rangos_pasillo": destination_owner_ranges,
        "tabla_destino_propietario_rangos_resumen": destination_owner_ranges_summary,
        "tabla_destino_altura00_visual": height00_positions,
        "tabla_destino_propietarios_abc_visual": owner_positions,
        "tabla_destino_overflow_propietarios": overflow,
    }
    for name, frame in tables.items():
        frame.to_csv(detail_dir / f"{name}.csv", index=False)
    (detail_dir / "workbook_tables_detail.json").write_text(
        json.dumps({name: _json_safe_records(frame) for name, frame in tables.items()}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    _render_markdown(
        detail_dir,
        coverage_final,
        source_summary,
        destination_summary,
        overflow,
        visual_outputs,
        owner_visual_outputs,
    )

    return {
        "detail_dir": detail_dir,
        "coverage_csv": detail_dir / "tabla_cobertura_propietarios.csv",
        "destination_ranges_csv": detail_dir / "tabla_destino_propietario_rangos_pasillo.csv",
        "source_special_csv": detail_dir / "tabla_origen_tramos_especiales.csv",
        "height00_visual_dir": visual_plot_dir,
        "height00_contact_sheet": visual_outputs["contact_sheet"],
        "owner_abc_visual_dir": owner_visual_dir,
        "owner_abc_contact_sheet": owner_visual_outputs["contact_sheet"],
        "summary_md": detail_dir / "explicacion_layout_detallado.md",
    }
