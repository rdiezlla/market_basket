from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .mahou_beneficio_layout_actual_2026 import (
    ACTIVE_AISLES,
    ACTIVE_AISLE_ORDER,
    EXPEDITION_X_METERS,
    EXPEDITION_Y_METERS,
    FORKLIFTS_CURRENT,
    FORKLIFT_ANNUAL_PRODUCTIVE_HOURS,
    FORKLIFT_REACH_EUR_MONTH,
    FTE_ANNUAL_HOURS,
    LABOUR_EUR_H,
    PERIOD_END,
    PERIOD_START,
    _build_counterfactual_assignment,
    _build_location_mapping,
    _distance,
    _json_safe_records,
    _load_sources,
    _owner_activity_table,
    _prepare_movements_2026,
    _prepare_stock_layout,
    _repo_map_rows,
)


OUTPUT_ROOT = "beneficio_layout_actual_2026_v2"
LATERAL_METERS_PER_AISLE = 5.50
LONGITUDINAL_METERS_PER_POSITION = 1.20
OBSERVED_DURATION_LOW_Q = 0.05
OBSERVED_DURATION_HIGH_Q = 0.95
USE_VARIABLE_HOUR_SAVINGS = True
OVERTIME_EUR_H = 27.50
TEMP_LABOUR_EUR_H = 24.00
VARIABLE_HOUR_RATE_SOURCE = "temp_labour"
VARIABLE_HOUR_RATE_EUR_H = TEMP_LABOUR_EUR_H if VARIABLE_HOUR_RATE_SOURCE == "temp_labour" else OVERTIME_EUR_H
RECOMMENDED_SCENARIO = "B_base_recomendado"

SCENARIO_CONFIG_V2: dict[str, dict[str, object]] = {
    "A_conservador": {
        "owner_sort": ["current_mean_aisle_order", "-picks_2026", "-lineas_2026", "-footprint_locations"],
        "location_order": "current_order",
        "counterfactual_route": "observed_order",
        "seconds_per_meter": 1.05,
        "seconds_per_aisle_change": 20.0,
        "seconds_per_stop": 6.5,
        "seconds_per_owner_fragment": 12.0,
        "seconds_per_discontinuous_block": 16.0,
        "seconds_per_search_event": 9.0,
        "seconds_per_maneuver": 11.0,
        "seconds_per_return_to_route": 12.0,
        "description": (
            "Agrupa por propietario con una mejora prudente: menos dispersion, "
            "pero sin asumir una secuenciacion especialmente afinada."
        ),
    },
    "B_base_recomendado": {
        "owner_sort": ["-picks_2026", "-lineas_2026", "-footprint_locations", "current_mean_aisle_order"],
        "location_order": "frequency_first",
        "counterfactual_route": "observed_order",
        "seconds_per_meter": 1.15,
        "seconds_per_aisle_change": 34.0,
        "seconds_per_stop": 8.5,
        "seconds_per_owner_fragment": 22.0,
        "seconds_per_discontinuous_block": 30.0,
        "seconds_per_search_event": 14.0,
        "seconds_per_maneuver": 18.0,
        "seconds_per_return_to_route": 22.0,
        "description": (
            "Agrupa por propietario, acerca los mas rotadores a expedicion y "
            "captura el beneficio operativo de reducir cambios, maniobras y reorientaciones."
        ),
    },
    "C_agresivo": {
        "owner_sort": ["-picks_2026", "-lineas_2026", "-footprint_locations", "current_mean_aisle_order"],
        "location_order": "frequency_first",
        "counterfactual_route": "spatial_sweep",
        "seconds_per_meter": 1.25,
        "seconds_per_aisle_change": 42.0,
        "seconds_per_stop": 10.0,
        "seconds_per_owner_fragment": 28.0,
        "seconds_per_discontinuous_block": 38.0,
        "seconds_per_search_event": 18.0,
        "seconds_per_maneuver": 22.0,
        "seconds_per_return_to_route": 28.0,
        "description": (
            "Agrupa por propietario y asume un aprovechamiento alto del bloque: "
            "mejor secuenciacion, menos retrocesos y menos reenganche de ruta."
        ),
    },
}

TIME_COMPONENT_LABELS = {
    "seconds_meters": "menos_metros",
    "seconds_aisle_change": "menos_cambios_pasillo",
    "seconds_stop": "menos_stops",
    "seconds_owner_fragment": "menos_fragmentacion_propietario",
    "seconds_discontinuous_block": "menos_bloques_discontinuos",
    "seconds_search": "menos_busqueda_reorientacion",
    "seconds_maneuver": "menos_maniobra",
    "seconds_return_route": "menos_reenganche_ruta",
}
TIME_COMPONENT_COLUMNS = list(TIME_COMPONENT_LABELS)


@dataclass(frozen=True)
class BenefitPathsV2:
    base_dir: Path
    output_dir: Path
    plots_dir: Path


def _ensure_directories_v2(base_dir: Path) -> BenefitPathsV2:
    output_dir = base_dir / "output" / OUTPUT_ROOT
    plots_dir = output_dir / "plots"
    output_dir.mkdir(parents=True, exist_ok=True)
    plots_dir.mkdir(parents=True, exist_ok=True)
    return BenefitPathsV2(base_dir=base_dir, output_dir=output_dir, plots_dir=plots_dir)


def _ordered_route_frame(frame: pd.DataFrame, mode: str, x_col: str, y_col: str) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    if mode == "spatial_sweep":
        return frame.sort_values([x_col, y_col, "line_id"]).reset_index(drop=True)
    return frame.sort_values(["start_time", "end_time", "line_id"]).reset_index(drop=True)


def _count_contiguous_blocks(values: list[int]) -> int:
    unique_sorted = sorted({int(value) for value in values if pd.notna(value)})
    if not unique_sorted:
        return 0
    blocks = 1
    for previous, current in zip(unique_sorted, unique_sorted[1:]):
        if current - previous > 1:
            blocks += 1
    return blocks


def _count_aisle_changes(aisle_sequence: list[int]) -> int:
    if not aisle_sequence:
        return 0
    changes = 0
    previous = aisle_sequence[0]
    for current in aisle_sequence[1:]:
        if current != previous:
            changes += 1
        previous = current
    return changes


def _count_route_reengagements(aisle_sequence: list[int]) -> int:
    if not aisle_sequence:
        return 0
    run_counts: dict[int, int] = {}
    previous = object()
    for aisle in aisle_sequence:
        if aisle != previous:
            run_counts[int(aisle)] = run_counts.get(int(aisle), 0) + 1
        previous = aisle
    return int(sum(max(count - 1, 0) for count in run_counts.values()))


def _aisle_span_from_orders(orders: pd.Series) -> int:
    numeric = pd.to_numeric(orders, errors="coerce").dropna()
    if numeric.empty:
        return 0
    return int(numeric.max() - numeric.min())


def _mad_from_orders(orders: pd.Series) -> float:
    numeric = pd.to_numeric(orders, errors="coerce").dropna()
    if numeric.empty:
        return 0.0
    return float(np.abs(numeric - numeric.mean()).mean())


def _owner_layout_profile(slot_frame: pd.DataFrame) -> pd.DataFrame:
    if slot_frame.empty:
        return pd.DataFrame(
            columns=[
                "owner",
                "owner_name",
                "pasillos_layout",
                "num_pasillos_layout",
                "bloques_discontinuos_layout",
                "amplitud_pasillos_layout",
                "posicion_media_layout",
                "dispersion_layout",
                "footprint_locations_layout",
            ]
        )
    base = slot_frame.copy()
    base["aisle"] = pd.to_numeric(base["aisle"], errors="coerce")
    base["position"] = pd.to_numeric(base["position"], errors="coerce")
    base["aisle_order"] = pd.to_numeric(base["aisle_order"], errors="coerce")
    base["owner"] = pd.to_numeric(base["owner"], errors="coerce")
    base["owner_name"] = base["owner_name"].astype(str).replace("nan", "").fillna("")
    rows: list[dict[str, object]] = []
    for owner, frame in base.groupby("owner"):
        aisles = sorted(int(value) for value in frame["aisle"].dropna().unique())
        orders = frame["aisle_order"]
        rows.append(
            {
                "owner": int(owner),
                "owner_name": frame["owner_name"].replace("", pd.NA).dropna().iloc[0] if frame["owner_name"].replace("", pd.NA).dropna().any() else "",
                "pasillos_layout": ",".join(str(value) for value in aisles),
                "num_pasillos_layout": int(len(aisles)),
                "bloques_discontinuos_layout": int(_count_contiguous_blocks(aisles)),
                "amplitud_pasillos_layout": _aisle_span_from_orders(orders),
                "posicion_media_layout": round(float(pd.to_numeric(frame["position"], errors="coerce").dropna().mean()), 2),
                "dispersion_layout": round(_mad_from_orders(orders), 4),
                "footprint_locations_layout": int(frame["location"].nunique()),
            }
        )
    return pd.DataFrame(rows).sort_values(["num_pasillos_layout", "footprint_locations_layout", "owner"], ascending=[False, False, True]).reset_index(drop=True)


def _prepare_movements_2026_v2(movements: pd.DataFrame, owner_names: dict[int, str]) -> tuple[pd.DataFrame, dict[str, float]]:
    frame = _prepare_movements_2026(movements, owner_names).copy()
    frame["line_duration_sec_raw"] = (frame["end_time"] - frame["start_time"]).dt.total_seconds()
    valid = frame["line_duration_sec_raw"].where(frame["line_duration_sec_raw"] > 0).dropna()
    if valid.empty:
        clip_low = 60.0
        clip_high = 600.0
    else:
        clip_low = float(valid.quantile(OBSERVED_DURATION_LOW_Q))
        clip_high = float(valid.quantile(OBSERVED_DURATION_HIGH_Q))
        if clip_high <= clip_low:
            clip_low = float(valid.median())
            clip_high = float(valid.quantile(0.99))
    fallback = max(clip_low, 1.0)
    frame["line_duration_sec_effective"] = frame["line_duration_sec_raw"].clip(lower=clip_low, upper=clip_high)
    frame["line_duration_sec_effective"] = frame["line_duration_sec_effective"].fillna(fallback)
    return frame, {"duration_clip_low_sec": round(clip_low, 4), "duration_clip_high_sec": round(clip_high, 4)}


def _component_seconds(
    scenario: str,
    meters: float,
    aisle_changes: int,
    stops: int,
    owner_fragment_proxy: float,
    discontinuous_block_proxy: float,
    search_events: float,
    maneuver_events: float,
    route_reengagements: int,
) -> dict[str, float]:
    cfg = SCENARIO_CONFIG_V2[scenario]
    components = {
        "seconds_meters": float(meters) * float(cfg["seconds_per_meter"]),
        "seconds_aisle_change": float(aisle_changes) * float(cfg["seconds_per_aisle_change"]),
        "seconds_stop": float(stops) * float(cfg["seconds_per_stop"]),
        "seconds_owner_fragment": float(owner_fragment_proxy) * float(cfg["seconds_per_owner_fragment"]),
        "seconds_discontinuous_block": float(discontinuous_block_proxy) * float(cfg["seconds_per_discontinuous_block"]),
        "seconds_search": float(search_events) * float(cfg["seconds_per_search_event"]),
        "seconds_maneuver": float(maneuver_events) * float(cfg["seconds_per_maneuver"]),
        "seconds_return_route": float(route_reengagements) * float(cfg["seconds_per_return_to_route"]),
    }
    components["model_seconds_total"] = float(sum(components.values()))
    return components


def _driver_metrics(
    ordered_frame: pd.DataFrame,
    aisle_col: str,
    location_col: str,
    x_col: str,
    y_col: str,
    owner_num_pasillos: int,
    owner_bloques: int,
) -> dict[str, float]:
    if ordered_frame.empty:
        return {
            "metros": 0.0,
            "stops": 0,
            "pasillos_tocados": 0,
            "cambios_pasillo": 0,
            "reenganches_ruta": 0,
            "owner_fragment_proxy": 0.0,
            "bloque_discontinuo_proxy": 0.0,
            "search_events": 0.0,
            "maneuver_events": 0.0,
        }

    aisles = [int(value) for value in pd.to_numeric(ordered_frame[aisle_col], errors="coerce").dropna().tolist()]
    stops = int(ordered_frame[location_col].astype(str).nunique())
    pasillos_tocados = int(len(set(aisles)))
    cambios_pasillo = _count_aisle_changes(aisles)
    reenganches = _count_route_reengagements(aisles)
    touched_share = pasillos_tocados / max(owner_num_pasillos, 1)
    owner_fragment_proxy = max(owner_num_pasillos - 1, 0) * touched_share
    bloque_discontinuo_proxy = max(owner_bloques - 1, 0) * touched_share
    search_events = max(pasillos_tocados - 1, 0) + reenganches
    maneuver_events = cambios_pasillo + max(stops - 1, 0)
    points = list(zip(ordered_frame[x_col].astype(float), ordered_frame[y_col].astype(float)))
    return {
        "metros": float(_distance(points)),
        "stops": stops,
        "pasillos_tocados": pasillos_tocados,
        "cambios_pasillo": int(cambios_pasillo),
        "reenganches_ruta": int(reenganches),
        "owner_fragment_proxy": float(owner_fragment_proxy),
        "bloque_discontinuo_proxy": float(bloque_discontinuo_proxy),
        "search_events": float(search_events),
        "maneuver_events": float(maneuver_events),
    }


def _layout_tables_v2(
    current_profile: pd.DataFrame,
    contra_profile: pd.DataFrame,
    activity: pd.DataFrame,
    base_assignment: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    actual_layout = current_profile.merge(activity, on="owner", how="outer")
    actual_layout["owner_name"] = actual_layout["owner_name"].fillna("")
    actual_layout["picks_2026"] = actual_layout["picks_2026"].fillna(0).astype(int)
    actual_layout["lineas_2026"] = actual_layout["lineas_2026"].fillna(0).astype(int)
    actual_layout["num_pasillos_layout"] = actual_layout["num_pasillos_layout"].fillna(0).astype(int)
    actual_layout["bloques_discontinuos_layout"] = actual_layout["bloques_discontinuos_layout"].fillna(0).astype(int)
    actual_layout["amplitud_pasillos_layout"] = actual_layout["amplitud_pasillos_layout"].fillna(0).astype(int)
    actual_layout["dispersion_layout"] = actual_layout["dispersion_layout"].fillna(0.0)
    actual_layout["posicion_media_layout"] = actual_layout["posicion_media_layout"].fillna(0.0)
    actual_layout = (
        actual_layout[
            [
                "owner",
                "owner_name",
                "pasillos_layout",
                "num_pasillos_layout",
                "bloques_discontinuos_layout",
                "amplitud_pasillos_layout",
                "dispersion_layout",
                "posicion_media_layout",
                "picks_2026",
                "lineas_2026",
            ]
        ]
        .rename(
            columns={
                "owner": "propietario",
                "pasillos_layout": "pasillos_actuales",
                "num_pasillos_layout": "num_pasillos_actuales",
                "bloques_discontinuos_layout": "bloques_discontinuos_actuales",
                "amplitud_pasillos_layout": "amplitud_pasillos_actual",
                "dispersion_layout": "dispersion_actual",
                "posicion_media_layout": "posicion_media_actual",
            }
        )
        .sort_values(["picks_2026", "lineas_2026", "num_pasillos_actuales", "propietario"], ascending=[False, False, False, True])
        .reset_index(drop=True)
    )

    contra = contra_profile.merge(activity, on="owner", how="left").merge(
        base_assignment[
            [
                "propietario",
                "pasillo_objetivo_1",
                "pasillo_objetivo_2",
                "num_pasillos_objetivo",
                "criterio_asignacion",
                "justificacion",
            ]
        ],
        left_on="owner",
        right_on="propietario",
        how="left",
    )
    contra["picks_2026"] = contra["picks_2026"].fillna(0).astype(int)
    contra["lineas_2026"] = contra["lineas_2026"].fillna(0).astype(int)
    contra["num_pasillos_layout"] = contra["num_pasillos_layout"].fillna(0).astype(int)
    contra["bloques_discontinuos_layout"] = contra["bloques_discontinuos_layout"].fillna(0).astype(int)
    contra["amplitud_pasillos_layout"] = contra["amplitud_pasillos_layout"].fillna(0).astype(int)
    contra["dispersion_layout"] = contra["dispersion_layout"].fillna(0.0)
    contra["posicion_media_layout"] = contra["posicion_media_layout"].fillna(0.0)
    contrafactual_layout = (
        contra[
            [
                "owner",
                "owner_name",
                "pasillo_objetivo_1",
                "pasillo_objetivo_2",
                "num_pasillos_objetivo",
                "pasillos_layout",
                "bloques_discontinuos_layout",
                "amplitud_pasillos_layout",
                "picks_2026",
                "lineas_2026",
                "criterio_asignacion",
                "justificacion",
            ]
        ]
        .rename(
            columns={
                "owner": "propietario",
                "pasillos_layout": "pasillos_objetivo",
                "bloques_discontinuos_layout": "bloques_discontinuos_objetivo",
                "amplitud_pasillos_layout": "amplitud_pasillos_objetivo",
            }
        )
        .sort_values(["pasillo_objetivo_1", "propietario"], ascending=[True, True])
        .reset_index(drop=True)
    )
    return actual_layout, contrafactual_layout


def _simulate_scenario_v2(
    scenario: str,
    movements_2026: pd.DataFrame,
    owner_footprint: pd.DataFrame,
    activity: pd.DataFrame,
    capacity_by_aisle: pd.DataFrame,
    slot_catalog: pd.DataFrame,
    owner_profile_current: pd.DataFrame,
) -> dict[str, object]:
    assignment, slot_assignment = _build_counterfactual_assignment(
        scenario=scenario,
        owner_footprint=owner_footprint,
        activity=activity,
        capacity_by_aisle=capacity_by_aisle,
        slot_catalog=slot_catalog,
    )
    mapping = _build_location_mapping(scenario, movements_2026, slot_assignment)
    target_profile = _owner_layout_profile(slot_assignment)
    frame = movements_2026.merge(mapping, on=["owner", "location"], how="left")
    frame["target_location"] = frame["target_location"].fillna(frame["location"])
    frame["target_aisle"] = pd.to_numeric(frame["target_aisle"], errors="coerce").fillna(frame["aisle"]).astype(int)
    frame["target_position"] = pd.to_numeric(frame["target_position"], errors="coerce").fillna(frame["position"])
    frame["target_x_m"] = pd.to_numeric(frame["target_x_m"], errors="coerce").fillna(frame["x_actual_m"])
    frame["target_y_m"] = pd.to_numeric(frame["target_y_m"], errors="coerce").fillna(frame["y_actual_m"])
    frame["target_aisle_order"] = frame["target_aisle"].map(ACTIVE_AISLE_ORDER)

    current_lookup = owner_profile_current.set_index("owner").to_dict(orient="index")
    target_lookup = target_profile.set_index("owner").to_dict(orient="index")
    transaction_rows: list[dict[str, object]] = []

    for transaction_id, tx_frame in frame.groupby("transaction_id", sort=False):
        owner = int(tx_frame["owner"].iloc[0])
        current_profile = current_lookup.get(
            owner,
            {
                "owner_name": tx_frame["owner_name"].iloc[0],
                "num_pasillos_layout": 1,
                "bloques_discontinuos_layout": 1,
            },
        )
        contra_profile = target_lookup.get(
            owner,
            {
                "owner_name": tx_frame["owner_name"].iloc[0],
                "num_pasillos_layout": 1,
                "bloques_discontinuos_layout": 1,
            },
        )
        actual_ordered = _ordered_route_frame(tx_frame, "observed_order", "x_actual_m", "y_actual_m")
        contra_ordered = _ordered_route_frame(
            tx_frame,
            str(SCENARIO_CONFIG_V2[scenario]["counterfactual_route"]),
            "target_x_m",
            "target_y_m",
        )
        actual_metrics = _driver_metrics(
            actual_ordered,
            aisle_col="aisle",
            location_col="location",
            x_col="x_actual_m",
            y_col="y_actual_m",
            owner_num_pasillos=int(current_profile.get("num_pasillos_layout", 1)),
            owner_bloques=int(current_profile.get("bloques_discontinuos_layout", 1)),
        )
        contra_metrics = _driver_metrics(
            contra_ordered,
            aisle_col="target_aisle",
            location_col="target_location",
            x_col="target_x_m",
            y_col="target_y_m",
            owner_num_pasillos=int(contra_profile.get("num_pasillos_layout", 1)),
            owner_bloques=int(contra_profile.get("bloques_discontinuos_layout", 1)),
        )
        actual_components = _component_seconds(
            scenario,
            meters=actual_metrics["metros"],
            aisle_changes=int(actual_metrics["cambios_pasillo"]),
            stops=int(actual_metrics["stops"]),
            owner_fragment_proxy=float(actual_metrics["owner_fragment_proxy"]),
            discontinuous_block_proxy=float(actual_metrics["bloque_discontinuo_proxy"]),
            search_events=float(actual_metrics["search_events"]),
            maneuver_events=float(actual_metrics["maneuver_events"]),
            route_reengagements=int(actual_metrics["reenganches_ruta"]),
        )
        contra_components = _component_seconds(
            scenario,
            meters=contra_metrics["metros"],
            aisle_changes=int(contra_metrics["cambios_pasillo"]),
            stops=int(contra_metrics["stops"]),
            owner_fragment_proxy=float(contra_metrics["owner_fragment_proxy"]),
            discontinuous_block_proxy=float(contra_metrics["bloque_discontinuo_proxy"]),
            search_events=float(contra_metrics["search_events"]),
            maneuver_events=float(contra_metrics["maneuver_events"]),
            route_reengagements=int(contra_metrics["reenganches_ruta"]),
        )
        transaction_rows.append(
            {
                "transaction_id": transaction_id,
                "propietario": owner,
                "owner_name": tx_frame["owner_name"].iloc[0],
                "lineas_transaccion": int(len(tx_frame)),
                "picks_2026_owner": int(tx_frame["transaction_id"].nunique()),
                "metros_actuales": round(actual_metrics["metros"], 2),
                "metros_contrafactuales": round(contra_metrics["metros"], 2),
                "ahorro_metros": round(actual_metrics["metros"] - contra_metrics["metros"], 2),
                "pasillos_tocados_actual": int(actual_metrics["pasillos_tocados"]),
                "pasillos_tocados_contrafactual": int(contra_metrics["pasillos_tocados"]),
                "cambios_pasillo_actual": int(actual_metrics["cambios_pasillo"]),
                "cambios_pasillo_contrafactual": int(contra_metrics["cambios_pasillo"]),
                "stops_actual": int(actual_metrics["stops"]),
                "stops_contrafactual": int(contra_metrics["stops"]),
                "reenganches_ruta_actual": int(actual_metrics["reenganches_ruta"]),
                "reenganches_ruta_contrafactual": int(contra_metrics["reenganches_ruta"]),
                "num_pasillos_owner_actual": int(current_profile.get("num_pasillos_layout", 1)),
                "num_pasillos_owner_contrafactual": int(contra_profile.get("num_pasillos_layout", 1)),
                "bloques_owner_actual": int(current_profile.get("bloques_discontinuos_layout", 1)),
                "bloques_owner_contrafactual": int(contra_profile.get("bloques_discontinuos_layout", 1)),
                "amplitud_owner_actual": int(current_profile.get("amplitud_pasillos_layout", 0)),
                "amplitud_owner_contrafactual": int(contra_profile.get("amplitud_pasillos_layout", 0)),
                "owner_fragment_proxy_actual": round(float(actual_metrics["owner_fragment_proxy"]), 4),
                "owner_fragment_proxy_contrafactual": round(float(contra_metrics["owner_fragment_proxy"]), 4),
                "bloque_discontinuo_proxy_actual": round(float(actual_metrics["bloque_discontinuo_proxy"]), 4),
                "bloque_discontinuo_proxy_contrafactual": round(float(contra_metrics["bloque_discontinuo_proxy"]), 4),
                "search_events_actual": round(float(actual_metrics["search_events"]), 4),
                "search_events_contrafactual": round(float(contra_metrics["search_events"]), 4),
                "maneuver_events_actual": round(float(actual_metrics["maneuver_events"]), 4),
                "maneuver_events_contrafactual": round(float(contra_metrics["maneuver_events"]), 4),
                "observed_duration_sec": round(float(tx_frame["line_duration_sec_effective"].sum()), 4),
                "model_seconds_actual": round(float(actual_components["model_seconds_total"]), 4),
                "model_seconds_contrafactual": round(float(contra_components["model_seconds_total"]), 4),
                "escenario": scenario,
                **{f"{column}_actual": round(float(actual_components[column]), 4) for column in TIME_COMPONENT_COLUMNS},
                **{f"{column}_contrafactual": round(float(contra_components[column]), 4) for column in TIME_COMPONENT_COLUMNS},
            }
        )

    transaction_table = pd.DataFrame(transaction_rows).sort_values(["propietario", "transaction_id"]).reset_index(drop=True)
    observed_total_seconds = float(transaction_table["observed_duration_sec"].sum())
    model_total_seconds = float(transaction_table["model_seconds_actual"].sum())
    calibration_factor = observed_total_seconds / model_total_seconds if model_total_seconds > 0 else 1.0
    transaction_table["calibration_factor"] = calibration_factor
    transaction_table["tiempo_actual_total_sec"] = transaction_table["observed_duration_sec"]
    transaction_table["tiempo_contrafactual_total_sec"] = transaction_table["model_seconds_contrafactual"] * calibration_factor
    transaction_table["ahorro_tiempo_sec"] = transaction_table["tiempo_actual_total_sec"] - transaction_table["tiempo_contrafactual_total_sec"]
    transaction_table["tiempo_actual_total_h"] = transaction_table["tiempo_actual_total_sec"] / 3600
    transaction_table["tiempo_contrafactual_total_h"] = transaction_table["tiempo_contrafactual_total_sec"] / 3600
    transaction_table["ahorro_tiempo_h"] = transaction_table["ahorro_tiempo_sec"] / 3600
    for column in TIME_COMPONENT_COLUMNS:
        transaction_table[f"{column}_actual_calibrado"] = transaction_table[f"{column}_actual"] * calibration_factor
        transaction_table[f"{column}_contrafactual_calibrado"] = transaction_table[f"{column}_contrafactual"] * calibration_factor
        transaction_table[f"delta_{column}"] = (
            transaction_table[f"{column}_actual_calibrado"] - transaction_table[f"{column}_contrafactual_calibrado"]
        )
    return {
        "assignment": assignment,
        "slot_assignment": slot_assignment,
        "location_mapping": mapping,
        "owner_profile_target": target_profile,
        "transaction_drivers": transaction_table,
        "calibration_factor": calibration_factor,
    }


def _owner_driver_table(
    scenario: str,
    transaction_table: pd.DataFrame,
    actual_layout: pd.DataFrame,
    contrafactual_layout: pd.DataFrame,
) -> pd.DataFrame:
    owner = (
        transaction_table.groupby(["propietario", "owner_name"], as_index=False, dropna=False)
        .agg(
            picks_2026=("transaction_id", "nunique"),
            lineas_2026=("lineas_transaccion", "sum"),
            horas_actuales=("tiempo_actual_total_h", "sum"),
            horas_contrafactuales=("tiempo_contrafactual_total_h", "sum"),
            ahorro_horas=("ahorro_tiempo_h", "sum"),
            metros_actuales=("metros_actuales", "sum"),
            metros_contrafactuales=("metros_contrafactuales", "sum"),
            ahorro_metros=("ahorro_metros", "sum"),
            pasillos_tocados_med_actual=("pasillos_tocados_actual", "mean"),
            pasillos_tocados_med_contrafactual=("pasillos_tocados_contrafactual", "mean"),
            cambios_pasillo_med_actual=("cambios_pasillo_actual", "mean"),
            cambios_pasillo_med_contrafactual=("cambios_pasillo_contrafactual", "mean"),
            reenganches_med_actual=("reenganches_ruta_actual", "mean"),
            reenganches_med_contrafactual=("reenganches_ruta_contrafactual", "mean"),
            fragmentacion_proxy_med_actual=("owner_fragment_proxy_actual", "mean"),
            fragmentacion_proxy_med_contrafactual=("owner_fragment_proxy_contrafactual", "mean"),
            busqueda_proxy_med_actual=("search_events_actual", "mean"),
            busqueda_proxy_med_contrafactual=("search_events_contrafactual", "mean"),
            maniobra_proxy_med_actual=("maneuver_events_actual", "mean"),
            maniobra_proxy_med_contrafactual=("maneuver_events_contrafactual", "mean"),
        )
        .sort_values("ahorro_horas", ascending=False)
        .reset_index(drop=True)
    )
    owner["transacciones_h_actual"] = np.where(owner["horas_actuales"] > 0, owner["picks_2026"] / owner["horas_actuales"], 0.0)
    owner["transacciones_h_contrafactual"] = np.where(owner["horas_contrafactuales"] > 0, owner["picks_2026"] / owner["horas_contrafactuales"], 0.0)
    owner["lineas_h_actual"] = np.where(owner["horas_actuales"] > 0, owner["lineas_2026"] / owner["horas_actuales"], 0.0)
    owner["lineas_h_contrafactual"] = np.where(owner["horas_contrafactuales"] > 0, owner["lineas_2026"] / owner["horas_contrafactuales"], 0.0)
    owner["uplift_productividad_pct"] = np.where(
        owner["lineas_h_actual"] > 0,
        (owner["lineas_h_contrafactual"] / owner["lineas_h_actual"]) - 1,
        0.0,
    )

    current_columns = [
        "propietario",
        "pasillos_actuales",
        "num_pasillos_actuales",
        "bloques_discontinuos_actuales",
        "amplitud_pasillos_actual",
        "dispersion_actual",
    ]
    target_columns = [
        "propietario",
        "pasillos_objetivo",
        "num_pasillos_objetivo",
        "bloques_discontinuos_objetivo",
        "amplitud_pasillos_objetivo",
    ]
    owner = owner.merge(actual_layout[current_columns], on="propietario", how="left").merge(
        contrafactual_layout[target_columns], on="propietario", how="left"
    )
    owner["escenario"] = scenario
    return owner


def _distance_base_table_v2(owner_driver_base: pd.DataFrame) -> pd.DataFrame:
    frame = owner_driver_base.copy()
    frame["ahorro_pct"] = np.where(frame["metros_actuales"] > 0, frame["ahorro_metros"] / frame["metros_actuales"], 0.0)
    frame["soporte"] = (
        "modelo operativo v2: distancia + cambios de pasillo + fragmentacion + "
        "busqueda/maniobra, calibrado con tiempos observados 2026"
    )
    return frame[
        [
            "propietario",
            "metros_actuales",
            "metros_contrafactuales",
            "ahorro_metros",
            "ahorro_pct",
            "soporte",
        ]
    ].sort_values("ahorro_metros", ascending=False).reset_index(drop=True)


def _productivity_table(
    scenario_tables: dict[str, dict[str, object]],
    movements_2026: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    productivity_rows: list[dict[str, object]] = []
    capacity_rows: list[dict[str, object]] = []
    total_transactions = int(movements_2026["transaction_id"].nunique())
    total_lines = int(len(movements_2026))

    for scenario, tables in scenario_tables.items():
        transaction_table = tables["transaction_drivers"]
        hours_actual = float(transaction_table["tiempo_actual_total_h"].sum())
        hours_contra = float(transaction_table["tiempo_contrafactual_total_h"].sum())
        tx_h_actual = total_transactions / hours_actual if hours_actual > 0 else 0.0
        tx_h_contra = total_transactions / hours_contra if hours_contra > 0 else 0.0
        lines_h_actual = total_lines / hours_actual if hours_actual > 0 else 0.0
        lines_h_contra = total_lines / hours_contra if hours_contra > 0 else 0.0
        uplift_tx_pct = (tx_h_contra / tx_h_actual - 1) if tx_h_actual > 0 else 0.0
        uplift_lines_pct = (lines_h_contra / lines_h_actual - 1) if lines_h_actual > 0 else 0.0
        extra_tx_same_hours = max(hours_actual * tx_h_contra - total_transactions, 0.0)
        extra_lines_same_hours = max(hours_actual * lines_h_contra - total_lines, 0.0)
        productivity_rows.append(
            {
                "escenario": scenario,
                "transacciones_2026": total_transactions,
                "lineas_2026": total_lines,
                "horas_actuales": round(hours_actual, 4),
                "horas_contrafactuales": round(hours_contra, 4),
                "horas_evitable_productividad": round(hours_actual - hours_contra, 4),
                "transacciones_h_actual": round(tx_h_actual, 4),
                "transacciones_h_contrafactual": round(tx_h_contra, 4),
                "lineas_h_actual": round(lines_h_actual, 4),
                "lineas_h_contrafactual": round(lines_h_contra, 4),
                "uplift_productividad_tx_pct": round(uplift_tx_pct, 6),
                "uplift_productividad_lineas_pct": round(uplift_lines_pct, 6),
            }
        )
        capacity_rows.append(
            {
                "escenario": scenario,
                "horas_actuales": round(hours_actual, 4),
                "horas_contrafactuales": round(hours_contra, 4),
                "horas_liberadas": round(hours_actual - hours_contra, 4),
                "capacidad_adicional_pedidos": round(extra_tx_same_hours, 2),
                "capacidad_adicional_lineas": round(extra_lines_same_hours, 2),
                "capacidad_adicional_pct": round(uplift_lines_pct, 6),
                "lineas_h_actual": round(lines_h_actual, 4),
                "lineas_h_contrafactual": round(lines_h_contra, 4),
            }
        )
    return pd.DataFrame(productivity_rows), pd.DataFrame(capacity_rows)


def _cost_and_resource_tables(
    scenario_tables: dict[str, dict[str, object]],
    owner_tables: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    equivalent_rows: list[dict[str, object]] = []
    realizable_rows: list[dict[str, object]] = []
    variable_rows: list[dict[str, object]] = []
    resource_rows: list[dict[str, object]] = []

    for scenario, owner_table in owner_tables.items():
        owner_cost = owner_table.copy()
        owner_cost["ahorro_personal_equivalente_eur"] = owner_cost["ahorro_horas"] * LABOUR_EUR_H
        owner_cost["ahorro_carretilla_equivalente_eur"] = (
            owner_cost["ahorro_horas"] / FORKLIFT_ANNUAL_PRODUCTIVE_HOURS * FORKLIFT_REACH_EUR_MONTH * 12
        )
        owner_cost["ahorro_total_equivalente_eur"] = (
            owner_cost["ahorro_personal_equivalente_eur"] + owner_cost["ahorro_carretilla_equivalente_eur"]
        )
        equivalent_rows.extend(
            owner_cost[
                [
                    "propietario",
                    "owner_name",
                    "ahorro_horas",
                    "ahorro_personal_equivalente_eur",
                    "ahorro_carretilla_equivalente_eur",
                    "ahorro_total_equivalente_eur",
                    "escenario",
                ]
            ]
            .rename(columns={"ahorro_horas": "horas_ahorradas"})
            .to_dict(orient="records")
        )

        total_hours = float(owner_cost["ahorro_horas"].sum())
        people_equiv = total_hours / FTE_ANNUAL_HOURS
        forklifts_equiv = total_hours / FORKLIFT_ANNUAL_PRODUCTIVE_HOURS
        people_realizable = int(math.floor(people_equiv))
        forklifts_realizable = int(min(math.floor(forklifts_equiv), FORKLIFTS_CURRENT))
        ahorro_personal_equiv = total_hours * LABOUR_EUR_H
        ahorro_carretilla_equiv = forklifts_equiv * FORKLIFT_REACH_EUR_MONTH * 12
        ahorro_personal_realizable = people_realizable * FTE_ANNUAL_HOURS * LABOUR_EUR_H
        ahorro_carretilla_realizable = forklifts_realizable * FORKLIFT_REACH_EUR_MONTH * 12
        horas_evitable_sin_baja = max(total_hours - people_realizable * FTE_ANNUAL_HOURS, 0.0)
        coste_variable_evitable = horas_evitable_sin_baja * VARIABLE_HOUR_RATE_EUR_H if USE_VARIABLE_HOUR_SAVINGS else 0.0

        realizable_rows.append(
            {
                "escenario": scenario,
                "horas_ahorradas_totales": round(total_hours, 4),
                "personas_equivalentes": round(people_equiv, 6),
                "personas_realizables": people_realizable,
                "ahorro_personal_realizable_eur": round(ahorro_personal_realizable, 2),
                "carretillas_equivalentes": round(forklifts_equiv, 6),
                "carretillas_realizables": forklifts_realizable,
                "carretillas_actuales": FORKLIFTS_CURRENT,
                "ahorro_carretilla_realizable_eur": int(ahorro_carretilla_realizable),
                "ahorro_total_realizable_eur": round(ahorro_personal_realizable + ahorro_carretilla_realizable, 2),
            }
        )
        variable_rows.append(
            {
                "escenario": scenario,
                "horas_evitable_sin_baja": round(horas_evitable_sin_baja, 4),
                "use_variable_hour_savings": USE_VARIABLE_HOUR_SAVINGS,
                "variable_hour_rate_source": VARIABLE_HOUR_RATE_SOURCE,
                "variable_hour_rate_eur_h": VARIABLE_HOUR_RATE_EUR_H,
                "coste_variable_evitable_eur": round(coste_variable_evitable, 2),
            }
        )
        resource_rows.append(
            {
                "escenario": scenario,
                "horas_ahorradas_totales": round(total_hours, 4),
                "personas_equivalentes": round(people_equiv, 6),
                "personas_realizables": people_realizable,
                "ahorro_personal_equivalente_eur": round(ahorro_personal_equiv, 2),
                "ahorro_personal_realizable_eur": round(ahorro_personal_realizable, 2),
                "carretillas_equivalentes": round(forklifts_equiv, 6),
                "carretillas_realizables": forklifts_realizable,
                "carretillas_actuales": FORKLIFTS_CURRENT,
                "ahorro_carretilla_equivalente_eur": round(ahorro_carretilla_equiv, 2),
                "ahorro_carretilla_realizable_eur": int(ahorro_carretilla_realizable),
                "ahorro_total_equivalente_eur": round(ahorro_personal_equiv + ahorro_carretilla_equiv, 2),
                "ahorro_total_realizable_eur": round(ahorro_personal_realizable + ahorro_carretilla_realizable, 2),
                "horas_evitable_sin_baja": round(horas_evitable_sin_baja, 4),
                "coste_variable_evitable_eur": round(coste_variable_evitable, 2),
            }
        )

    equivalent = pd.DataFrame(equivalent_rows)
    realizable = pd.DataFrame(realizable_rows)
    variable = pd.DataFrame(variable_rows)
    resources = pd.DataFrame(resource_rows)
    return equivalent, realizable, variable, resources


def _sensitivity_table_v2(
    scenario_tables: dict[str, dict[str, object]],
    productivity: pd.DataFrame,
    resources: pd.DataFrame,
    variable: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    productivity_lookup = productivity.set_index("escenario").to_dict(orient="index")
    resources_lookup = resources.set_index("escenario").to_dict(orient="index")
    variable_lookup = variable.set_index("escenario").to_dict(orient="index")
    for scenario, tables in scenario_tables.items():
        transaction_table = tables["transaction_drivers"]
        cfg = SCENARIO_CONFIG_V2[scenario]
        product_row = productivity_lookup[scenario]
        resource_row = resources_lookup[scenario]
        variable_row = variable_lookup[scenario]
        rows.append(
            {
                "escenario": scenario,
                "criterio_distancia": cfg["counterfactual_route"],
                "seconds_per_meter": cfg["seconds_per_meter"],
                "seconds_per_aisle_change": cfg["seconds_per_aisle_change"],
                "seconds_per_stop": cfg["seconds_per_stop"],
                "seconds_per_owner_fragment": cfg["seconds_per_owner_fragment"],
                "seconds_per_discontinuous_block": cfg["seconds_per_discontinuous_block"],
                "seconds_per_search_event": cfg["seconds_per_search_event"],
                "seconds_per_maneuver": cfg["seconds_per_maneuver"],
                "seconds_per_return_to_route": cfg["seconds_per_return_to_route"],
                "calibration_factor": round(float(tables["calibration_factor"]), 6),
                "ahorro_metros": round(float(transaction_table["ahorro_metros"].sum()), 2),
                "ahorro_horas": round(float(product_row["horas_evitable_productividad"]), 4),
                "ahorro_equivalente_eur": round(float(resource_row["ahorro_total_equivalente_eur"]), 2),
                "ahorro_realizable_eur": round(float(resource_row["ahorro_total_realizable_eur"]), 2),
                "coste_variable_evitable_eur": round(float(variable_row["coste_variable_evitable_eur"]), 2),
                "uplift_productividad_lineas_pct": float(product_row["uplift_productividad_lineas_pct"]),
                "capacidad_adicional_pct": float(product_row["uplift_productividad_lineas_pct"]),
            }
        )
    return pd.DataFrame(rows)


def _supuestos_table_v2(duration_clips: dict[str, float]) -> pd.DataFrame:
    rows: list[dict[str, object]] = [
        {
            "escenario": "global",
            "grupo": "geometria",
            "parametro": "lateral_metros_por_pasillo",
            "valor": LATERAL_METERS_PER_AISLE,
            "unidad": "m",
            "descripcion": "Coste lateral entre pasillos contiguos del almacen actual.",
        },
        {
            "escenario": "global",
            "grupo": "geometria",
            "parametro": "longitudinal_metros_por_posicion",
            "valor": LONGITUDINAL_METERS_PER_POSITION,
            "unidad": "m",
            "descripcion": "Modulo longitudinal por posicion.",
        },
        {
            "escenario": "global",
            "grupo": "coste",
            "parametro": "coste_personal",
            "valor": LABOUR_EUR_H,
            "unidad": "eur_h",
            "descripcion": "Coste de personal aportado por negocio.",
        },
        {
            "escenario": "global",
            "grupo": "coste",
            "parametro": "coste_carretilla_mensual",
            "valor": FORKLIFT_REACH_EUR_MONTH,
            "unidad": "eur_mes",
            "descripcion": "Alquiler mensual por carretilla actual.",
        },
        {
            "escenario": "global",
            "grupo": "coste",
            "parametro": "carretillas_actuales",
            "valor": FORKLIFTS_CURRENT,
            "unidad": "uds",
            "descripcion": "Dotacion actual de carretillas.",
        },
        {
            "escenario": "global",
            "grupo": "observado",
            "parametro": "duration_clip_low_sec",
            "valor": duration_clips["duration_clip_low_sec"],
            "unidad": "s",
            "descripcion": "Cota inferior usada para winsorizar duraciones reales de linea.",
        },
        {
            "escenario": "global",
            "grupo": "observado",
            "parametro": "duration_clip_high_sec",
            "valor": duration_clips["duration_clip_high_sec"],
            "unidad": "s",
            "descripcion": "Cota superior usada para winsorizar duraciones reales de linea.",
        },
        {
            "escenario": "global",
            "grupo": "variable",
            "parametro": "use_variable_hour_savings",
            "valor": USE_VARIABLE_HOUR_SAVINGS,
            "unidad": "bool",
            "descripcion": "Activa la lectura de ahorro variable evitable cuando no se elimina una persona completa.",
        },
        {
            "escenario": "global",
            "grupo": "variable",
            "parametro": "variable_hour_rate_eur_h",
            "valor": VARIABLE_HOUR_RATE_EUR_H,
            "unidad": "eur_h",
            "descripcion": f"Tarifa variable usada ({VARIABLE_HOUR_RATE_SOURCE}).",
        },
    ]
    for scenario, config in SCENARIO_CONFIG_V2.items():
        for parameter in [
            "seconds_per_meter",
            "seconds_per_aisle_change",
            "seconds_per_stop",
            "seconds_per_owner_fragment",
            "seconds_per_discontinuous_block",
            "seconds_per_search_event",
            "seconds_per_maneuver",
            "seconds_per_return_to_route",
        ]:
            rows.append(
                {
                    "escenario": scenario,
                    "grupo": "drivers_operativos",
                    "parametro": parameter,
                    "valor": config[parameter],
                    "unidad": "s",
                    "descripcion": str(config["description"]),
                }
            )
    return pd.DataFrame(rows)


def _heatmap_tables(
    slot_catalog: pd.DataFrame,
    slot_assignment: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    current = (
        slot_catalog[slot_catalog["occupied"] & slot_catalog["owner"].le(100)]
        .groupby(["owner", "aisle"], as_index=False)["location"]
        .nunique()
        .rename(columns={"owner": "propietario", "aisle": "pasillo", "location": "ubicaciones"})
    )
    contrafactual = (
        slot_assignment.groupby(["owner", "aisle"], as_index=False)["location"]
        .nunique()
        .rename(columns={"owner": "propietario", "aisle": "pasillo", "location": "ubicaciones"})
    )
    return current, contrafactual


def _plot_owner_segment_map(frame: pd.DataFrame, assignment_col: str, title: str, path: Path) -> None:
    plot_frame = frame.copy()
    plot_frame["y"] = np.arange(len(plot_frame))
    fig, ax = plt.subplots(figsize=(12, max(6, len(plot_frame) * 0.22)))
    for row in plot_frame.itertuples(index=False):
        raw_value = getattr(row, assignment_col)
        aisles = [int(value) for value in str(raw_value).split(",") if value and value.lower() != "nan"]
        for aisle in aisles:
            x = ACTIVE_AISLE_ORDER[int(aisle)]
            color = "#2b6cb0" if assignment_col == "pasillos_actuales" else "#2f855a"
            ax.broken_barh([(x, 1.0)], (row.y - 0.4, 0.8), facecolors=color)
    ax.set_yticks(plot_frame["y"])
    ax.set_yticklabels([str(int(value)) for value in plot_frame["propietario"]])
    ax.set_xticks(range(len(ACTIVE_AISLES)))
    ax.set_xticklabels([str(value) for value in ACTIVE_AISLES])
    ax.set_xlabel("Pasillo")
    ax.set_ylabel("Propietario")
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_heatmap(frame: pd.DataFrame, title: str, path: Path) -> None:
    pivot = frame.pivot_table(index="propietario", columns="pasillo", values="ubicaciones", fill_value=0).sort_index()
    fig, ax = plt.subplots(figsize=(12, max(6, pivot.shape[0] * 0.22)))
    image = ax.imshow(pivot.values, aspect="auto", cmap="YlGnBu")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels([str(int(value)) for value in pivot.index])
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels([str(int(value)) for value in pivot.columns])
    ax.set_xlabel("Pasillo")
    ax.set_ylabel("Propietario")
    ax.set_title(title)
    fig.colorbar(image, ax=ax, fraction=0.03, pad=0.02)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_bars(frame: pd.DataFrame, metric: str, title: str, xlabel: str, path: Path, color: str) -> None:
    subset = frame.sort_values(metric, ascending=True).tail(15)
    fig, ax = plt.subplots(figsize=(11, 7))
    labels = [str(int(value)) for value in subset["propietario"]]
    ax.barh(labels, subset[metric], color=color)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_scatter(frame: pd.DataFrame, x_metric: str, y_metric: str, title: str, xlabel: str, ylabel: str, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(frame[x_metric], frame[y_metric], color="#2b6cb0", alpha=0.75)
    for row in frame.sort_values(y_metric, ascending=False).head(12).itertuples(index=False):
        ax.annotate(str(int(row.propietario)), (getattr(row, x_metric), getattr(row, y_metric)), fontsize=8, xytext=(4, 3), textcoords="offset points")
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_waterfall_v2(
    recommended_resource_row: pd.Series,
    recommended_variable_row: pd.Series,
    recommended_capacity_row: pd.Series,
    path: Path,
) -> None:
    stages = [
        "Ahorro\npersonal eq.",
        "Ahorro\ncarretilla eq.",
        "Ahorro\nrealizable",
        "Variable\nevitable",
    ]
    values = [
        float(recommended_resource_row["ahorro_personal_equivalente_eur"]),
        float(recommended_resource_row["ahorro_carretilla_equivalente_eur"]),
        float(recommended_resource_row["ahorro_total_realizable_eur"]),
        float(recommended_variable_row["coste_variable_evitable_eur"]),
    ]
    colors = ["#2b6cb0", "#ed8936", "#2f855a", "#805ad5"]
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(stages, values, color=colors)
    ax.set_title(
        "Waterfall del beneficio v2 (eq. vs realizable vs variable)\n"
        f"Capacidad adicional con misma dotacion: {recommended_capacity_row['capacidad_adicional_pct']:.1%}"
    )
    ax.set_ylabel("EUR")
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _build_workbook_manifest(tables: dict[str, pd.DataFrame], path: Path) -> None:
    path.write_text(
        json.dumps({name: _json_safe_records(frame) for name, frame in tables.items()}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _render_markdown_v2(
    paths: BenefitPathsV2,
    repo_map: pd.DataFrame,
    actual_layout: pd.DataFrame,
    contrafactual_layout: pd.DataFrame,
    distance_base: pd.DataFrame,
    productivity: pd.DataFrame,
    capacity: pd.DataFrame,
    equivalent: pd.DataFrame,
    realizable: pd.DataFrame,
    variable: pd.DataFrame,
    resources: pd.DataFrame,
    sensitivity: pd.DataFrame,
    owner_driver_base: pd.DataFrame,
) -> Path:
    recommended_sensitivity = sensitivity[sensitivity["escenario"] == RECOMMENDED_SCENARIO].iloc[0]
    recommended_productivity = productivity[productivity["escenario"] == RECOMMENDED_SCENARIO].iloc[0]
    recommended_capacity = capacity[capacity["escenario"] == RECOMMENDED_SCENARIO].iloc[0]
    recommended_resources = resources[resources["escenario"] == RECOMMENDED_SCENARIO].iloc[0]
    recommended_variable = variable[variable["escenario"] == RECOMMENDED_SCENARIO].iloc[0]
    max_sensitivity = sensitivity.sort_values("ahorro_equivalente_eur", ascending=False).iloc[0]
    reused = repo_map[repo_map["se_reutiliza"].isin(["si", "parcial"])]

    contribution_rows = []
    for column, label in TIME_COMPONENT_LABELS.items():
        contribution_rows.append(
            {
                "componente": label,
                "horas": owner_driver_base.merge(
                    distance_base[["propietario"]],
                    on="propietario",
                    how="inner",
                )
                .pipe(lambda _: 0.0),
            }
        )
    base_tx = owner_driver_base.copy()
    driver_tx_path = paths.output_dir / "tabla_drivers_operativos_por_transaccion.csv"
    if driver_tx_path.exists():
        driver_tx = pd.read_csv(driver_tx_path)
        driver_tx = driver_tx[driver_tx["escenario"] == RECOMMENDED_SCENARIO].copy()
        contribution_rows = [
            {
                "componente": TIME_COMPONENT_LABELS[column],
                "horas": float(driver_tx[f"delta_{column}"].sum()) / 3600,
            }
            for column in TIME_COMPONENT_COLUMNS
        ]
    contribution_text = "\n".join(
        [
            f"- {row['componente']}: {row['horas']:.2f} h"
            for row in sorted(contribution_rows, key=lambda item: item["horas"], reverse=True)
        ]
    )

    output = f"""# beneficio_layout_actual_2026_v2

## 1. por que el modelo anterior era conservador

- La v1 convertia sobre todo metros en horas y, por tanto, monetizaba poco del beneficio real de agrupar por propietario.
- La v1 medía bien la dispersion espacial, pero no hacia visible el castigo operativo de los cambios de pasillo, la fragmentacion del propietario, la maniobra, la busqueda y el reenganche de ruta.
- La v2 mantiene el hecho observado y el contrafactual, pero usa un modelo de tiempo operativo con drivers explicitamente parametrizados y calibrado contra tiempos reales observados en 2026.

## 2. que parte del repo se reutilizo

- Se han reutilizado como base estructural: {", ".join(reused["archivo"].tolist())}.
- Se ha mantenido la logica actual vs contrafactual del almacen actual 2026.
- La nueva pieza se limita a ampliar el modelo operativo y economico, sin romper la v1.

## 3. como esta construido el repo actualmente

- `main.py` sigue siendo el entrypoint del pipeline general de market basket.
- `transactions.py` aporta la unidad `pedido externo + propietario`, que sigue siendo la unidad principal de este caso.
- `mahou_beneficio_layout_actual_2026.py` queda conservado como referencia v1.
- `mahou_beneficio_layout_actual_2026_v2.py` es la capa nueva para decision operativa de gerente de plataforma.

## 4. como se modelo el almacen actual

- Hecho observado: foto real `17-04-2026.xlsx` y movimientos `PI` 2026 del almacen actual.
- Geometria fija del modelo: 5.50 m por salto entre pasillos contiguos y 1.20 m por posicion longitudinal.
- Tiempo observado real: se usa la duracion de las lineas de movimiento 2026 winsorizada para reducir outliers extremos sin ocultarlos.
- El tiempo actual de referencia del modelo v2 se ancla al tiempo observado, no a una velocidad teorica de paseo.

## 5. como se construyo el layout contrafactual

- Cada propietario se agrupa en el minimo numero razonable de pasillos contiguos dentro del almacen actual.
- La capacidad por pasillo se mantiene tomada de la foto real de stock.
- En el escenario recomendado se prioriza hacia expedicion lo que mas rota en 2026.
- La simulacion no cambia de nave ni usa el layout destino.

## 6. ahorro en metros

- Escenario base recomendado: {recommended_sensitivity['ahorro_metros']:,.2f} m.
- Escenario conservador: {sensitivity[sensitivity['escenario']=='A_conservador']['ahorro_metros'].iloc[0]:,.2f} m.
- Escenario agresivo: {sensitivity[sensitivity['escenario']=='C_agresivo']['ahorro_metros'].iloc[0]:,.2f} m.

## 7. ahorro en horas

- Escenario base recomendado: {recommended_sensitivity['ahorro_horas']:,.2f} h.
- El tiempo ya no sale solo de metros / velocidad: incorpora cambios de pasillo, stops, fragmentacion, bloques discontinuos, busqueda, maniobra y reenganche de ruta.
- Horas actuales para el volumen 2026 observado: {recommended_productivity['horas_actuales']:,.2f} h.
- Horas contrafactuales para el mismo volumen: {recommended_productivity['horas_contrafactuales']:,.2f} h.

## 8. ahorro equivalente, realizable y variable

- Ahorro equivalente base recomendado: {recommended_resources['ahorro_total_equivalente_eur']:,.2f} EUR.
- Ahorro realizable directo base recomendado: {recommended_resources['ahorro_total_realizable_eur']:,.2f} EUR.
- Coste variable evitable base recomendado: {recommended_variable['coste_variable_evitable_eur']:,.2f} EUR.
- Ahorro equivalente != ahorro realizable.
- Si no se elimina una persona o una carretilla entera, el valor sigue existiendo como horas evitadas, menor necesidad de extra/refuerzo y capacidad adicional.

## 9. productividad y capacidad

- Transacciones/hora actual: {recommended_productivity['transacciones_h_actual']:,.2f}
- Transacciones/hora contrafactual: {recommended_productivity['transacciones_h_contrafactual']:,.2f}
- Lineas/hora actual: {recommended_productivity['lineas_h_actual']:,.2f}
- Lineas/hora contrafactual: {recommended_productivity['lineas_h_contrafactual']:,.2f}
- Uplift de productividad lineas/hora: {recommended_productivity['uplift_productividad_lineas_pct']:.1%}
- Capacidad adicional con la misma dotacion: {recommended_capacity['capacidad_adicional_pct']:.1%}
- Pedidos adicionales absorbibles con la misma dotacion: {recommended_capacity['capacidad_adicional_pedidos']:,.2f}
- Lineas adicionales absorbibles con la misma dotacion: {recommended_capacity['capacidad_adicional_lineas']:,.2f}

## 10. personas equivalentes y carretillas equivalentes

- Personas equivalentes base: {recommended_resources['personas_equivalentes']:.4f}
- Personas realizables base: {int(recommended_resources['personas_realizables'])}
- Carretillas equivalentes base: {recommended_resources['carretillas_equivalentes']:.4f}
- Carretillas realizables base: {int(recommended_resources['carretillas_realizables'])}
- El ahorro realizable de carretilla solo se presenta en enteros completos.

## 11. de donde viene el beneficio operativo

{contribution_text}

## 12. sensibilidad, riesgos y conclusion ejecutiva

- Escenario recomendado: {RECOMMENDED_SCENARIO}.
- Maximo ahorro razonable del set de sensibilidad: {max_sensitivity['ahorro_equivalente_eur']:,.2f} EUR equivalentes.
- Riesgos principales: la foto de stock es un corte, la reasignacion contrafactual sigue siendo una simulacion, y parte del beneficio puede capturarse como capacidad y no como baja directa.
- Conclusion ejecutiva: agrupar por propietario en el almacen actual mejora de forma visible la productividad real porque reduce la dispersion operativa y no solo la distancia recorrida.

ahorro base recomendado: {recommended_resources['ahorro_total_equivalente_eur']:,.2f} EUR equivalentes, {recommended_sensitivity['ahorro_horas']:,.2f} h, {recommended_sensitivity['ahorro_metros']:,.2f} m
maximo ahorro razonable: {max_sensitivity['ahorro_equivalente_eur']:,.2f} EUR equivalentes, {max_sensitivity['ahorro_horas']:,.2f} h, {max_sensitivity['ahorro_metros']:,.2f} m
que supuesto cambia mas el resultado: la severidad de penalizacion de fragmentacion/cambios de pasillo junto con la secuenciacion contrafactual dentro del bloque del propietario
"""
    path = paths.output_dir / "beneficio_layout_actual_2026_v2.md"
    path.write_text(output, encoding="utf-8")
    return path


def run_beneficio_layout_actual_2026_v2(base_dir: Path) -> dict[str, str]:
    paths = _ensure_directories_v2(base_dir)
    sources = _load_sources(base_dir)
    repo_map = _repo_map_rows(base_dir, sources)
    slot_catalog, capacity_by_aisle, owner_footprint, owner_names = _prepare_stock_layout(sources["stock"])
    movements_2026, duration_clips = _prepare_movements_2026_v2(sources["movements"], owner_names)
    activity = _owner_activity_table(movements_2026)
    current_slots = slot_catalog[slot_catalog["occupied"] & slot_catalog["owner"].le(100)].copy()
    owner_profile_current = _owner_layout_profile(current_slots)

    scenario_tables = {
        scenario: _simulate_scenario_v2(
            scenario=scenario,
            movements_2026=movements_2026,
            owner_footprint=owner_footprint,
            activity=activity,
            capacity_by_aisle=capacity_by_aisle,
            slot_catalog=slot_catalog,
            owner_profile_current=owner_profile_current,
        )
        for scenario in SCENARIO_CONFIG_V2
    }

    base_assignment = scenario_tables[RECOMMENDED_SCENARIO]["assignment"].copy()
    base_target_profile = scenario_tables[RECOMMENDED_SCENARIO]["owner_profile_target"].copy()
    actual_layout, contrafactual_layout = _layout_tables_v2(
        current_profile=owner_profile_current,
        contra_profile=base_target_profile,
        activity=activity,
        base_assignment=base_assignment,
    )

    owner_tables = {
        scenario: _owner_driver_table(
            scenario=scenario,
            transaction_table=tables["transaction_drivers"],
            actual_layout=actual_layout,
            contrafactual_layout=contrafactual_layout,
        )
        for scenario, tables in scenario_tables.items()
    }

    productivity, capacity = _productivity_table(scenario_tables, movements_2026)
    equivalent, realizable, variable, resources = _cost_and_resource_tables(scenario_tables, owner_tables)
    sensitivity = _sensitivity_table_v2(scenario_tables, productivity, resources, variable)
    supuestos = _supuestos_table_v2(duration_clips)
    owner_driver_base = owner_tables[RECOMMENDED_SCENARIO].copy()
    distance_base = _distance_base_table_v2(owner_driver_base)
    heatmap_actual, heatmap_contra = _heatmap_tables(slot_catalog, scenario_tables[RECOMMENDED_SCENARIO]["slot_assignment"])
    transaction_driver_all = pd.concat(
        [tables["transaction_drivers"] for tables in scenario_tables.values()],
        ignore_index=True,
    )
    owner_driver_all = pd.concat(list(owner_tables.values()), ignore_index=True)

    tables: dict[str, pd.DataFrame] = {
        "tabla_repo_map": repo_map,
        "tabla_layout_actual_propietario": actual_layout,
        "tabla_layout_contrafactual_propietario": contrafactual_layout,
        "tabla_distancia_actual_vs_contrafactual": distance_base,
        "tabla_drivers_operativos_por_transaccion": transaction_driver_all,
        "tabla_drivers_operativos_por_propietario": owner_driver_all,
        "tabla_productividad_actual_vs_contrafactual": productivity,
        "tabla_impacto_capacidad": capacity,
        "tabla_impacto_coste_equivalente": equivalent,
        "tabla_impacto_coste_realizable": realizable,
        "tabla_impacto_coste_variable_evitable": variable,
        "tabla_recursos_equivalentes_vs_realizables": resources,
        "tabla_sensibilidad_v2": sensitivity,
        "tabla_supuestos_modelo_v2": supuestos,
        "tabla_heatmap_pasillo_propietario_actual": heatmap_actual,
        "tabla_heatmap_pasillo_propietario_contrafactual": heatmap_contra,
    }

    outputs: dict[str, str] = {}
    for name, frame in tables.items():
        table_path = paths.output_dir / f"{name}.csv"
        frame.to_csv(table_path, index=False, encoding="utf-8-sig")
        outputs[name] = str(table_path)

    current_map_path = paths.plots_dir / "mapa_dispersion_actual_por_propietario.png"
    contra_map_path = paths.plots_dir / "mapa_contrafactual_por_propietario.png"
    time_bar_path = paths.plots_dir / "barras_tiempo_ahorrado_por_propietario.png"
    uplift_bar_path = paths.plots_dir / "barras_uplift_productividad_por_propietario.png"
    scatter_picks_path = paths.plots_dir / "scatter_picks_vs_ahorro_tiempo.png"
    scatter_fragment_path = paths.plots_dir / "scatter_fragmentacion_actual_vs_ahorro.png"
    waterfall_path = paths.plots_dir / "waterfall_ahorro_total_v2.png"
    heatmap_current_path = paths.plots_dir / "heatmap_pasillo_propietario_actual.png"
    heatmap_contra_path = paths.plots_dir / "heatmap_pasillo_propietario_contrafactual.png"

    _plot_owner_segment_map(
        frame=actual_layout.sort_values(["picks_2026", "lineas_2026", "propietario"], ascending=[False, False, True]).head(30),
        assignment_col="pasillos_actuales",
        title="Mapa de dispersion actual por propietario",
        path=current_map_path,
    )
    _plot_owner_segment_map(
        frame=contrafactual_layout.sort_values(["pasillo_objetivo_1", "propietario"]).head(30),
        assignment_col="pasillos_objetivo",
        title="Mapa contrafactual agrupado por propietario",
        path=contra_map_path,
    )
    _plot_bars(
        frame=owner_driver_base,
        metric="ahorro_horas",
        title="Ahorro de tiempo por propietario (escenario base)",
        xlabel="Horas ahorradas",
        path=time_bar_path,
        color="#2f855a",
    )
    _plot_bars(
        frame=owner_driver_base,
        metric="uplift_productividad_pct",
        title="Uplift de productividad por propietario (escenario base)",
        xlabel="Uplift productividad",
        path=uplift_bar_path,
        color="#3182ce",
    )
    _plot_scatter(
        frame=owner_driver_base,
        x_metric="picks_2026",
        y_metric="ahorro_horas",
        title="Picks 2026 vs ahorro de tiempo por propietario",
        xlabel="Picks 2026",
        ylabel="Horas ahorradas",
        path=scatter_picks_path,
    )
    _plot_scatter(
        frame=owner_driver_base,
        x_metric="num_pasillos_actuales",
        y_metric="ahorro_horas",
        title="Fragmentacion actual vs ahorro de tiempo",
        xlabel="Num. pasillos actuales del propietario",
        ylabel="Horas ahorradas",
        path=scatter_fragment_path,
    )
    _plot_waterfall_v2(
        recommended_resource_row=resources[resources["escenario"] == RECOMMENDED_SCENARIO].iloc[0],
        recommended_variable_row=variable[variable["escenario"] == RECOMMENDED_SCENARIO].iloc[0],
        recommended_capacity_row=capacity[capacity["escenario"] == RECOMMENDED_SCENARIO].iloc[0],
        path=waterfall_path,
    )
    _plot_heatmap(heatmap_actual, "Heatmap pasillo x propietario actual", heatmap_current_path)
    _plot_heatmap(heatmap_contra, "Heatmap pasillo x propietario contrafactual", heatmap_contra_path)

    outputs["mapa_dispersion_actual_por_propietario"] = str(current_map_path)
    outputs["mapa_contrafactual_por_propietario"] = str(contra_map_path)
    outputs["barras_tiempo_ahorrado_por_propietario"] = str(time_bar_path)
    outputs["barras_uplift_productividad_por_propietario"] = str(uplift_bar_path)
    outputs["scatter_picks_vs_ahorro_tiempo"] = str(scatter_picks_path)
    outputs["scatter_fragmentacion_actual_vs_ahorro"] = str(scatter_fragment_path)
    outputs["waterfall_ahorro_total_v2"] = str(waterfall_path)
    outputs["heatmap_pasillo_propietario_actual"] = str(heatmap_current_path)
    outputs["heatmap_pasillo_propietario_contrafactual"] = str(heatmap_contra_path)

    summary_md = _render_markdown_v2(
        paths=paths,
        repo_map=repo_map,
        actual_layout=actual_layout,
        contrafactual_layout=contrafactual_layout,
        distance_base=distance_base,
        productivity=productivity,
        capacity=capacity,
        equivalent=equivalent,
        realizable=realizable,
        variable=variable,
        resources=resources,
        sensitivity=sensitivity,
        owner_driver_base=owner_driver_base,
    )
    outputs["beneficio_layout_actual_2026_v2_md"] = str(summary_md)

    manifest_path = paths.output_dir / "workbook_tables_detail.json"
    _build_workbook_manifest(tables, manifest_path)
    outputs["workbook_manifest"] = str(manifest_path)
    return outputs
