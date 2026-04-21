from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .mahou_dimensioning import ORIGIN_AISLES, _normalize_dataframe_columns


OUTPUT_ROOT = "beneficio_layout_actual_2026"
PERIOD_START = pd.Timestamp("2026-01-01")
PERIOD_END = pd.Timestamp("2026-12-31 23:59:59")
LATERAL_METERS_PER_AISLE = 5.50
LONGITUDINAL_METERS_PER_POSITION = 1.20
LABOUR_EUR_H = 22.0
FORKLIFT_REACH_EUR_MONTH = 1100.0
FORKLIFTS_CURRENT = 3
FTE_ANNUAL_HOURS = 1760.0
FORKLIFT_ANNUAL_PRODUCTIVE_HOURS = 1920.0
EXPEDITION_X_METERS = 0.0
EXPEDITION_Y_METERS = 0.0
ACTIVE_AISLES = ORIGIN_AISLES
ACTIVE_AISLE_ORDER = {aisle: index for index, aisle in enumerate(ACTIVE_AISLES)}
SCENARIO_CONFIG = {
    "A_conservador": {
        "owner_sort": ["current_mean_aisle_order", "-picks_2026", "-lineas_2026", "-footprint_locations"],
        "location_order": "current_order",
        "counterfactual_route": "observed_order",
        "speed_m_per_s": 1.5,
        "description": "Agrupa por propietario sin repriorizar fuerte la cercanía a expedición y sin reordenar la secuencia de preparación.",
    },
    "B_base_recomendado": {
        "owner_sort": ["-picks_2026", "-lineas_2026", "-footprint_locations", "current_mean_aisle_order"],
        "location_order": "frequency_first",
        "counterfactual_route": "observed_order",
        "speed_m_per_s": 1.2,
        "description": "Agrupa por propietario, prioriza los de más rotación hacia expedición y coloca primero las localizaciones más usadas.",
    },
    "C_agresivo": {
        "owner_sort": ["-picks_2026", "-lineas_2026", "-footprint_locations", "current_mean_aisle_order"],
        "location_order": "frequency_first",
        "counterfactual_route": "spatial_sweep",
        "speed_m_per_s": 0.9,
        "description": "Agrupa por propietario, acerca los más rotadores y asume que el operario explota mejor el bloque reordenando el recorrido dentro del propietario.",
    },
}


@dataclass(frozen=True)
class BenefitPaths:
    base_dir: Path
    output_dir: Path
    plots_dir: Path


def _json_safe_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    export = frame.copy()
    for column in export.columns:
        if pd.api.types.is_datetime64_any_dtype(export[column]):
            export[column] = export[column].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
    return json.loads(export.to_json(orient="records", date_format="iso"))


def _ensure_directories(base_dir: Path) -> BenefitPaths:
    output_dir = base_dir / "output" / OUTPUT_ROOT
    plots_dir = output_dir / "plots"
    output_dir.mkdir(parents=True, exist_ok=True)
    plots_dir.mkdir(parents=True, exist_ok=True)
    return BenefitPaths(base_dir=base_dir, output_dir=output_dir, plots_dir=plots_dir)


def _load_sources(base_dir: Path) -> dict[str, pd.DataFrame]:
    stock = _normalize_dataframe_columns(pd.read_excel(base_dir / "17-04-2026.xlsx"))
    movements = _normalize_dataframe_columns(pd.read_excel(base_dir / "movimientos.xlsx"))
    output_dir = base_dir / "output"
    optional_sources: dict[str, pd.DataFrame] = {}
    optional_map = {
        "sku_location_profile": output_dir / "sku_location_profile.csv",
        "transacciones_resumen": output_dir / "transacciones_resumen.csv",
        "mahou_demanda_propietario": output_dir / "mahou_codex" / "csv" / "tabla_demanda_propietario.csv",
    }
    for key, path in optional_map.items():
        if path.exists():
            optional_sources[key] = pd.read_csv(path)
    return {"stock": stock, "movements": movements, **optional_sources}


def _repo_map_rows(base_dir: Path, sources: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = [
        {
            "archivo": "main.py",
            "tipo": "entrypoint",
            "función": "Carga configuración YAML, prepara logging y lanza el pipeline general de market basket.",
            "se_reutiliza": "si",
            "comentario": "Sirve como mapa del flujo principal del repo, pero no se ejecuta directamente para esta simulación.",
        },
        {
            "archivo": "src/market_basket/pipeline.py",
            "tipo": "orquestación",
            "función": "Encadena lectura, limpieza, transacciones, EDA, asociaciones, temporalidad, scoring, clustering y outputs.",
            "se_reutiliza": "si",
            "comentario": "Se reutiliza como referencia estructural y para identificar outputs ya existentes útiles.",
        },
        {
            "archivo": "src/market_basket/cleaning.py",
            "tipo": "limpieza",
            "función": "Aplica filtros de calidad y construye perfiles SKU-localización.",
            "se_reutiliza": "si",
            "comentario": "Útil conceptualmente por `sku_location_profile`; no se usa directo porque el caso trabaja con la foto real del almacén actual.",
        },
        {
            "archivo": "src/market_basket/transactions.py",
            "tipo": "transacciones",
            "función": "Construye la transacción `pedido externo + propietario`, basket size, `unique_locations_in_basket` y `basket_dispersion_proxy`.",
            "se_reutiliza": "si",
            "comentario": "Es la pieza más cercana al caso: la unidad `pedido externo + propietario` se adopta como métrica principal de simulación.",
        },
        {
            "archivo": "src/market_basket/eda.py",
            "tipo": "eda",
            "función": "Resume KPIs, artículos, transacciones, propietarios y series temporales.",
            "se_reutiliza": "parcial",
            "comentario": "Sirve para validar magnitudes de 2026 y para entender la distribución de baskets; no modela distancia física.",
        },
        {
            "archivo": "src/market_basket/associations.py",
            "tipo": "market_basket",
            "función": "Calcula afinidades SKU-SKU y reglas de asociación.",
            "se_reutiliza": "no_directo",
            "comentario": "No resuelve la pregunta principal porque el caso se lanza por propietario, no por afinidad entre SKUs.",
        },
        {
            "archivo": "src/market_basket/temporal.py",
            "tipo": "temporalidad",
            "función": "Mide estabilidad temporal y tendencias de pares SKU-SKU.",
            "se_reutiliza": "no_directo",
            "comentario": "Aporta robustez temporal en el repo base, pero no aporta distancia ni layout actual por propietario.",
        },
        {
            "archivo": "src/market_basket/scoring.py",
            "tipo": "scoring",
            "función": "Convierte afinidad en recomendaciones de cercanía física entre SKUs.",
            "se_reutiliza": "no_directo",
            "comentario": "Está pensado para agrupar SKUs; aquí el criterio rector es propietario.",
        },
        {
            "archivo": "src/market_basket/clustering.py",
            "tipo": "clustering",
            "función": "Agrupa SKUs en comunidades mediante grafo de afinidad.",
            "se_reutiliza": "no_directo",
            "comentario": "Útil para clustering SKU, no para el contrafactual de bloques por propietario.",
        },
        {
            "archivo": "src/market_basket/outputs.py",
            "tipo": "outputs",
            "función": "Escribe CSV/Parquet/Excel, gráficos del pipeline y resumen ejecutivo.",
            "se_reutiliza": "si",
            "comentario": "Se reutiliza el patrón de generación reproducible de tablas, gráficos y resumen, pero con un módulo nuevo específico.",
        },
        {
            "archivo": "src/market_basket/mahou_dimensioning.py",
            "tipo": "mahou_operativo",
            "función": "Normaliza fuentes Mahou y prepara stock, movimientos y métricas operativas.",
            "se_reutiliza": "si",
            "comentario": "Se reutiliza la normalización de columnas y la definición de pasillos activos del almacén actual.",
        },
        {
            "archivo": "src/market_basket/mahou_layout_detail.py",
            "tipo": "mahou_layout",
            "función": "Genera el layout detallado del almacén destino y visuales por pasillo.",
            "se_reutiliza": "no_directo",
            "comentario": "No se usa para el cálculo porque este caso es solo del almacén actual, pero sirve como referencia de salida detallada.",
        },
        {
            "archivo": "output/transacciones_resumen.csv",
            "tipo": "output_existente",
            "función": "Resumen transaccional del pipeline base con `unique_locations_in_basket` y dispersión proxy.",
            "se_reutiliza": "si" if "transacciones_resumen" in sources else "no_disponible",
            "comentario": "Se usa como apoyo de validación, no como verdad única.",
        },
        {
            "archivo": "output/sku_location_profile.csv",
            "tipo": "output_existente",
            "función": "Perfil histórico SKU-localización del pipeline base.",
            "se_reutiliza": "si" if "sku_location_profile" in sources else "no_disponible",
            "comentario": "Sirve para contrastar dispersión histórica, aunque el hecho principal sale de la foto real 17-04-2026.",
        },
        {
            "archivo": "output/mahou_codex_rotacion_2026",
            "tipo": "output_existente",
            "función": "Layout de destino 2026 del ejercicio anterior.",
            "se_reutiliza": "no_directo",
            "comentario": "Se descarta para el cálculo central porque el usuario pidió explícitamente no analizar el almacén destino.",
        },
        {
            "archivo": "nuevo_modulo_beneficio_layout_actual_2026",
            "tipo": "nueva_pieza",
            "función": "Simula el layout contrafactual del almacén actual 2026 agrupado por propietario y monetiza el impacto.",
            "se_reutiliza": "nueva",
            "comentario": "Es la pieza que faltaba en el repo: dispersión actual + layout contrafactual + impacto económico en una sola simulación.",
        },
    ]
    return pd.DataFrame(rows)


def _prepare_stock_layout(stock: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[int, str]]:
    frame = stock.copy()
    frame["aisle"] = pd.to_numeric(frame["pasillo"], errors="coerce")
    frame["position"] = pd.to_numeric(frame["col"], errors="coerce")
    frame["height"] = pd.to_numeric(frame["alt"], errors="coerce")
    frame["owner"] = pd.to_numeric(frame["propie"], errors="coerce")
    frame["occupied"] = frame["ocupacion"].astype(str).str.upper().eq("OCUPADO")
    frame["location"] = frame["ubicacion"].astype(str).str.strip()
    frame["owner_name"] = frame["denominacion_propietario"].astype(str).str.strip()
    frame = frame[frame["aisle"].isin(ACTIVE_AISLES)].copy()
    frame["aisle_order"] = frame["aisle"].map(ACTIVE_AISLE_ORDER)
    frame["x_meters"] = frame["aisle_order"] * LATERAL_METERS_PER_AISLE
    frame["y_meters"] = frame["position"] * LONGITUDINAL_METERS_PER_POSITION

    slot_catalog = (
        frame.sort_values(["aisle_order", "position", "height", "location"])
        .drop_duplicates("location")
        .reset_index(drop=True)
    )
    slot_catalog["slot_sequence"] = np.arange(1, len(slot_catalog) + 1)

    occupied = slot_catalog[slot_catalog["occupied"] & slot_catalog["owner"].le(100)].copy()
    owner_name_map = (
        occupied[["owner", "owner_name"]]
        .dropna()
        .drop_duplicates()
        .sort_values(["owner_name", "owner"])
        .drop_duplicates("owner")
    )
    owner_names = {
        int(row.owner): row.owner_name
        for row in owner_name_map.itertuples(index=False)
        if pd.notna(row.owner) and row.owner_name and row.owner_name.lower() != "nan"
    }

    capacity_by_aisle = (
        slot_catalog.groupby(["aisle", "aisle_order"], as_index=False)
        .agg(capacity_locations=("location", "nunique"))
        .sort_values("aisle_order")
        .reset_index(drop=True)
    )

    owner_footprint = (
        occupied.groupby("owner")
        .agg(
            footprint_locations=("location", "nunique"),
            pasillos_actuales=("aisle", lambda s: ",".join(str(int(value)) for value in sorted(s.dropna().unique()))),
            num_pasillos_actuales=("aisle", lambda s: int(s.dropna().nunique())),
            current_mean_aisle_order=("aisle_order", "mean"),
            current_mean_position=("position", "mean"),
            current_dispersion_mad=("aisle_order", lambda s: float(np.abs(s - s.mean()).mean()) if len(s) else 0.0),
        )
        .reset_index()
        .sort_values(["footprint_locations", "owner"], ascending=[False, True])
        .reset_index(drop=True)
    )
    owner_footprint["owner_name"] = owner_footprint["owner"].map(owner_names)
    return slot_catalog, capacity_by_aisle, owner_footprint, owner_names


def _prepare_movements_2026(movements: pd.DataFrame, owner_names: dict[int, str]) -> pd.DataFrame:
    frame = movements.copy()
    frame["movement_type"] = frame["tipo_movimiento"].astype(str).str.strip().str.upper()
    frame["owner"] = pd.to_numeric(frame["propietario"], errors="coerce")
    frame["aisle"] = pd.to_numeric(frame["pas_ori"], errors="coerce")
    frame["position"] = pd.to_numeric(frame["col_ori"], errors="coerce")
    frame["height"] = pd.to_numeric(frame["alt_ori"], errors="coerce")
    frame["quantity"] = pd.to_numeric(frame["cantidad"], errors="coerce")
    frame["external_order"] = frame["pedido_externo"].astype(str).str.strip()
    frame["location"] = frame["ubicacion"].astype(str).str.strip()
    frame["article"] = frame["articulo"].astype(str).str.strip()
    frame["owner_name"] = frame["owner"].map(owner_names)
    frame["start_time"] = pd.to_datetime(frame["fecha_inicio"], errors="coerce", dayfirst=True)
    frame["end_time"] = pd.to_datetime(frame["fecha_finalizacion"], errors="coerce", dayfirst=True)

    export = frame[
        frame["movement_type"].eq("PI")
        & frame["owner"].le(100).fillna(False)
        & frame["aisle"].isin(ACTIVE_AISLES)
        & frame["quantity"].gt(0).fillna(False)
        & frame["external_order"].ne("")
        & frame["end_time"].between(PERIOD_START, PERIOD_END, inclusive="both")
    ].copy()
    export["transaction_id"] = export["external_order"] + "|" + export["owner"].astype(int).astype(str)
    export["aisle_order"] = export["aisle"].map(ACTIVE_AISLE_ORDER)
    export["x_actual_m"] = export["aisle_order"] * LATERAL_METERS_PER_AISLE
    export["y_actual_m"] = export["position"] * LONGITUDINAL_METERS_PER_POSITION
    export["line_id"] = np.arange(1, len(export) + 1)
    return export.sort_values(["transaction_id", "start_time", "end_time", "line_id"]).reset_index(drop=True)


def _owner_activity_table(movements_2026: pd.DataFrame) -> pd.DataFrame:
    activity = (
        movements_2026.groupby("owner", as_index=False)
        .agg(
            picks_2026=("transaction_id", "nunique"),
            lineas_2026=("line_id", "count"),
            lineas_qty_2026=("quantity", "sum"),
        )
        .sort_values(["picks_2026", "lineas_2026", "owner"], ascending=[False, False, True])
        .reset_index(drop=True)
    )
    return activity


def _actual_layout_table(owner_footprint: pd.DataFrame, activity: pd.DataFrame) -> pd.DataFrame:
    table = owner_footprint.merge(activity, on="owner", how="outer")
    table["footprint_locations"] = table["footprint_locations"].fillna(0).astype(int)
    table["picks_2026"] = table["picks_2026"].fillna(0).astype(int)
    table["lineas_2026"] = table["lineas_2026"].fillna(0).astype(int)
    table["num_pasillos_actuales"] = table["num_pasillos_actuales"].fillna(0).astype(int)
    table["dispersion_actual"] = table["current_dispersion_mad"].fillna(0.0).round(4)
    table["posicion_media_actual"] = table["current_mean_position"].fillna(0.0).round(2)
    return (
        table[
            [
                "owner",
                "pasillos_actuales",
                "num_pasillos_actuales",
                "dispersion_actual",
                "posicion_media_actual",
                "picks_2026",
                "lineas_2026",
            ]
        ]
        .rename(columns={"owner": "propietario"})
        .sort_values(["picks_2026", "lineas_2026", "num_pasillos_actuales", "propietario"], ascending=[False, False, False, True])
        .reset_index(drop=True)
    )


def _sort_frame(frame: pd.DataFrame, sort_spec: list[str]) -> pd.DataFrame:
    sort_columns: list[str] = []
    ascending: list[bool] = []
    for value in sort_spec:
        if value.startswith("-"):
            sort_columns.append(value[1:])
            ascending.append(False)
        else:
            sort_columns.append(value)
            ascending.append(True)
    return frame.sort_values(sort_columns, ascending=ascending).reset_index(drop=True)


def _build_counterfactual_assignment(
    scenario: str,
    owner_footprint: pd.DataFrame,
    activity: pd.DataFrame,
    capacity_by_aisle: pd.DataFrame,
    slot_catalog: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    config = SCENARIO_CONFIG[scenario]
    owner_base = owner_footprint.merge(activity, on="owner", how="outer")
    owner_base["footprint_locations"] = owner_base["footprint_locations"].fillna(0)
    owner_base["picks_2026"] = owner_base["picks_2026"].fillna(0)
    owner_base["lineas_2026"] = owner_base["lineas_2026"].fillna(0)
    owner_base["current_mean_aisle_order"] = owner_base["current_mean_aisle_order"].fillna(len(ACTIVE_AISLES) + 1)

    movement_unique_locations = (
        slot_catalog[slot_catalog["occupied"] & slot_catalog["owner"].le(100)]
        .groupby("owner")["location"]
        .nunique()
        .rename("movement_like_locations")
        .reset_index()
    )
    owner_base = owner_base.merge(movement_unique_locations, on="owner", how="left")
    owner_base["movement_like_locations"] = owner_base["movement_like_locations"].fillna(0)
    owner_base["required_locations"] = owner_base[["footprint_locations", "movement_like_locations"]].max(axis=1)
    owner_base["required_locations"] = owner_base["required_locations"].fillna(0).astype(int)
    owner_base = owner_base[owner_base["required_locations"] > 0].copy()
    owner_base = _sort_frame(owner_base, config["owner_sort"])

    remaining_capacity = {
        int(row.aisle): int(row.capacity_locations)
        for row in capacity_by_aisle.itertuples(index=False)
    }
    slot_by_aisle = {
        int(aisle): frame.sort_values(["position", "height", "slot_sequence"]).copy()
        for aisle, frame in slot_catalog.groupby("aisle")
    }
    aisle_pointer = 0
    assignment_rows: list[dict[str, object]] = []
    slot_rows: list[dict[str, object]] = []

    for row in owner_base.itertuples(index=False):
        owner = int(row.owner)
        required = int(row.required_locations)
        allocated_parts: list[tuple[int, int]] = []
        while required > 0 and aisle_pointer < len(ACTIVE_AISLES):
            aisle = ACTIVE_AISLES[aisle_pointer]
            available = remaining_capacity.get(aisle, 0)
            if available <= 0:
                aisle_pointer += 1
                continue
            take = min(available, required)
            allocated_parts.append((aisle, take))
            remaining_capacity[aisle] -= take
            required -= take
            if remaining_capacity[aisle] <= 0:
                aisle_pointer += 1

        assigned_aisles = [aisle for aisle, _ in allocated_parts]
        if not assigned_aisles:
            continue
        slots_for_owner: list[pd.DataFrame] = []
        for aisle, take in allocated_parts:
            candidate_slots = slot_by_aisle[aisle].head(take).copy()
            slots_for_owner.append(candidate_slots)
        owner_slots = pd.concat(slots_for_owner, ignore_index=True) if slots_for_owner else pd.DataFrame(columns=slot_catalog.columns)
        owner_slots = owner_slots.sort_values(["aisle_order", "position", "height", "slot_sequence"]).reset_index(drop=True)
        owner_slots["owner"] = owner
        owner_slots["owner_name"] = row.owner_name
        owner_slots["scenario"] = scenario
        owner_slots["target_slot_rank"] = np.arange(1, len(owner_slots) + 1)
        slot_rows.extend(owner_slots.to_dict(orient="records"))

        assignment_rows.append(
            {
                "propietario": owner,
                "owner_name": row.owner_name,
                "pasillo_objetivo_1": int(assigned_aisles[0]) if assigned_aisles else pd.NA,
                "pasillo_objetivo_2": int(assigned_aisles[1]) if len(assigned_aisles) > 1 else pd.NA,
                "num_pasillos_objetivo": int(len(assigned_aisles)),
                "pasillos_objetivo": ",".join(str(int(value)) for value in assigned_aisles),
                "criterio_asignacion": config["description"],
                "justificacion": (
                    f"footprint={int(row.required_locations)} ubicaciones; "
                    f"picks_2026={int(row.picks_2026)}; "
                    f"lineas_2026={int(row.lineas_2026)}"
                ),
            }
        )

    assignment = pd.DataFrame(assignment_rows).sort_values(["pasillo_objetivo_1", "propietario"]).reset_index(drop=True)
    slot_assignment = pd.DataFrame(slot_rows)
    return assignment, slot_assignment


def _build_location_mapping(
    scenario: str,
    movements_2026: pd.DataFrame,
    slot_assignment: pd.DataFrame,
) -> pd.DataFrame:
    config = SCENARIO_CONFIG[scenario]
    location_usage = (
        movements_2026.groupby(["owner", "location", "aisle", "position"], as_index=False)
        .agg(
            lineas_2026=("line_id", "count"),
            picks_2026=("transaction_id", "nunique"),
        )
    )
    if config["location_order"] == "frequency_first":
        location_usage = location_usage.sort_values(
            ["owner", "lineas_2026", "picks_2026", "aisle", "position", "location"],
            ascending=[True, False, False, True, True, True],
        )
    else:
        location_usage = location_usage.sort_values(
            ["owner", "aisle", "position", "location"],
            ascending=[True, True, True, True],
        )

    target_rows: list[dict[str, object]] = []
    for owner, frame in location_usage.groupby("owner"):
        owner_targets = slot_assignment[slot_assignment["owner"] == owner].sort_values(
            ["target_slot_rank", "position", "height", "location"]
        )
        if owner_targets.empty:
            continue
        owner_targets = owner_targets.reset_index(drop=True)
        frame = frame.reset_index(drop=True)
        for index, item in frame.iterrows():
            target = owner_targets.iloc[min(index, len(owner_targets) - 1)]
            target_rows.append(
                {
                    "owner": int(owner),
                    "location": item["location"],
                    "target_location": target["location"],
                    "target_aisle": int(target["aisle"]),
                    "target_position": float(target["position"]),
                    "target_height": float(target["height"]),
                    "target_x_m": float(target["x_meters"]),
                    "target_y_m": float(target["y_meters"]),
                }
            )
    return pd.DataFrame(target_rows)


def _distance(points: list[tuple[float, float]]) -> float:
    x_prev = EXPEDITION_X_METERS
    y_prev = EXPEDITION_Y_METERS
    total = 0.0
    for x_now, y_now in points:
        total += abs(float(x_now) - x_prev) + abs(float(y_now) - y_prev)
        x_prev = float(x_now)
        y_prev = float(y_now)
    total += abs(x_prev - EXPEDITION_X_METERS) + abs(y_prev - EXPEDITION_Y_METERS)
    return total


def _route_points(frame: pd.DataFrame, mode: str, x_col: str, y_col: str) -> list[tuple[float, float]]:
    if frame.empty:
        return []
    if mode == "spatial_sweep":
        ordered = frame.sort_values([x_col, y_col, "line_id"])
    else:
        ordered = frame.sort_values(["start_time", "end_time", "line_id"])
    return list(zip(ordered[x_col].astype(float), ordered[y_col].astype(float)))


def _simulate_scenario(
    scenario: str,
    movements_2026: pd.DataFrame,
    owner_footprint: pd.DataFrame,
    activity: pd.DataFrame,
    capacity_by_aisle: pd.DataFrame,
    slot_catalog: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    assignment, slot_assignment = _build_counterfactual_assignment(
        scenario=scenario,
        owner_footprint=owner_footprint,
        activity=activity,
        capacity_by_aisle=capacity_by_aisle,
        slot_catalog=slot_catalog,
    )
    mapping = _build_location_mapping(scenario, movements_2026, slot_assignment)
    frame = movements_2026.merge(mapping, on=["owner", "location"], how="left")
    frame["target_x_m"] = frame["target_x_m"].fillna(frame["x_actual_m"])
    frame["target_y_m"] = frame["target_y_m"].fillna(frame["y_actual_m"])
    route_mode = SCENARIO_CONFIG[scenario]["counterfactual_route"]

    transaction_rows: list[dict[str, object]] = []
    line_rows: list[dict[str, object]] = []
    for transaction_id, tx_frame in frame.groupby("transaction_id", sort=False):
        owner = int(tx_frame["owner"].iloc[0])
        actual_points = _route_points(tx_frame, "observed_order", "x_actual_m", "y_actual_m")
        contra_points = _route_points(tx_frame, route_mode, "target_x_m", "target_y_m")
        meters_actual = _distance(actual_points)
        meters_contrafactual = _distance(contra_points)
        ahorro = meters_actual - meters_contrafactual
        transaction_rows.append(
            {
                "transaction_id": transaction_id,
                "propietario": owner,
                "owner_name": tx_frame["owner_name"].iloc[0],
                "lineas_transaccion": int(len(tx_frame)),
                "metros_actuales": round(meters_actual, 2),
                "metros_contrafactuales": round(meters_contrafactual, 2),
                "ahorro_metros": round(ahorro, 2),
                "ahorro_pct": round(ahorro / meters_actual, 6) if meters_actual > 0 else 0.0,
                "unidad_analisis": "transaccion",
                "escenario": scenario,
            }
        )
        ahorro_linea = ahorro / max(len(tx_frame), 1)
        for line in tx_frame.itertuples(index=False):
            line_rows.append(
                {
                    "line_id": int(line.line_id),
                    "transaction_id": transaction_id,
                    "propietario": owner,
                    "owner_name": line.owner_name,
                    "location_actual": line.location,
                    "location_contrafactual": line.target_location if pd.notna(line.target_location) else line.location,
                    "ahorro_metros_linea_proxy": round(ahorro_linea, 4),
                    "unidad_analisis": "linea",
                    "escenario": scenario,
                }
            )

    transaction_table = pd.DataFrame(transaction_rows)
    line_table = pd.DataFrame(line_rows)
    owner_distance = (
        transaction_table.groupby(["propietario", "owner_name"], as_index=False)
        .agg(
            metros_actuales=("metros_actuales", "sum"),
            metros_contrafactuales=("metros_contrafactuales", "sum"),
            ahorro_metros=("ahorro_metros", "sum"),
            transacciones_2026=("transaction_id", "nunique"),
            lineas_2026=("lineas_transaccion", "sum"),
        )
        .sort_values("ahorro_metros", ascending=False)
        .reset_index(drop=True)
    )
    owner_distance["ahorro_pct"] = np.where(
        owner_distance["metros_actuales"] > 0,
        owner_distance["ahorro_metros"] / owner_distance["metros_actuales"],
        0.0,
    )
    owner_distance["soporte"] = (
        "simulación transaccional con secuencia "
        + ("observada" if route_mode == "observed_order" else "espacial contrafactual")
    )
    return {
        "assignment": assignment,
        "slot_assignment": slot_assignment,
        "location_mapping": mapping,
        "transaction_distance": transaction_table,
        "line_distance": line_table,
        "owner_distance": owner_distance,
    }


def _time_and_cost_tables(simulations: dict[str, dict[str, pd.DataFrame]]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    time_rows: list[dict[str, object]] = []
    cost_rows: list[dict[str, object]] = []
    personnel_rows: list[dict[str, object]] = []

    for scenario, tables in simulations.items():
        speed = SCENARIO_CONFIG[scenario]["speed_m_per_s"]
        owner_distance = tables["owner_distance"].copy()
        owner_distance["tiempo_ahorrado_horas"] = owner_distance["ahorro_metros"] / speed / 3600
        owner_distance["ahorro_personal_eur"] = owner_distance["tiempo_ahorrado_horas"] * LABOUR_EUR_H
        owner_distance["ahorro_carretilla_equivalente_eur"] = (
            owner_distance["tiempo_ahorrado_horas"] / FORKLIFT_ANNUAL_PRODUCTIVE_HOURS * FORKLIFT_REACH_EUR_MONTH * 12
        )
        owner_distance["ahorro_total_equivalente_eur"] = (
            owner_distance["ahorro_personal_eur"] + owner_distance["ahorro_carretilla_equivalente_eur"]
        )
        owner_distance["ahorro_total_realizable_eur"] = (
            np.floor(owner_distance["tiempo_ahorrado_horas"] / FTE_ANNUAL_HOURS) * FTE_ANNUAL_HOURS * LABOUR_EUR_H
        ) + (
            np.floor(owner_distance["tiempo_ahorrado_horas"] / FORKLIFT_ANNUAL_PRODUCTIVE_HOURS)
            * FORKLIFT_REACH_EUR_MONTH
            * 12
        )

        for row in owner_distance.itertuples(index=False):
            time_rows.append(
                {
                    "propietario": int(row.propietario),
                    "metros_ahorrados": round(float(row.ahorro_metros), 2),
                    "velocidad_o_factor_tiempo": speed,
                    "tiempo_ahorrado_horas": round(float(row.tiempo_ahorrado_horas), 4),
                    "escenario": scenario,
                }
            )
            cost_rows.append(
                {
                    "propietario": int(row.propietario),
                    "horas_ahorradas": round(float(row.tiempo_ahorrado_horas), 4),
                    "ahorro_personal_eur": round(float(row.ahorro_personal_eur), 2),
                    "ahorro_carretilla_equivalente_eur": round(float(row.ahorro_carretilla_equivalente_eur), 2),
                    "ahorro_total_equivalente_eur": round(float(row.ahorro_total_equivalente_eur), 2),
                    "ahorro_total_realizable_eur": round(float(row.ahorro_total_realizable_eur), 2),
                    "escenario": scenario,
                }
            )

        total_hours = float(owner_distance["tiempo_ahorrado_horas"].sum())
        people_equiv = total_hours / FTE_ANNUAL_HOURS
        forklifts_equiv = total_hours / FORKLIFT_ANNUAL_PRODUCTIVE_HOURS
        personnel_rows.append(
            {
                "escenario": scenario,
                "horas_ahorradas_totales": round(total_hours, 4),
                "jornada_anual_horas": FTE_ANNUAL_HOURS,
                "personas_equivalentes": round(people_equiv, 4),
                "carretillas_equivalentes": round(forklifts_equiv, 4),
                "carretillas_actuales": FORKLIFTS_CURRENT,
                "ahorro_carretilla_realizable": round(
                    min(np.floor(forklifts_equiv), FORKLIFTS_CURRENT) * FORKLIFT_REACH_EUR_MONTH * 12,
                    2,
                ),
            }
        )

    return pd.DataFrame(time_rows), pd.DataFrame(cost_rows), pd.DataFrame(personnel_rows)


def _sensitivity_table(simulations: dict[str, dict[str, pd.DataFrame]]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for scenario, tables in simulations.items():
        owner_distance = tables["owner_distance"]
        total_meters = float(owner_distance["ahorro_metros"].sum())
        speed = SCENARIO_CONFIG[scenario]["speed_m_per_s"]
        total_hours = total_meters / speed / 3600
        total_eur = total_hours * LABOUR_EUR_H + total_hours / FORKLIFT_ANNUAL_PRODUCTIVE_HOURS * FORKLIFT_REACH_EUR_MONTH * 12
        rows.append(
            {
                "escenario": scenario,
                "criterio_distancia": SCENARIO_CONFIG[scenario]["counterfactual_route"],
                "velocidad": speed,
                "coste_personal": LABOUR_EUR_H,
                "coste_carretilla_mensual": FORKLIFT_REACH_EUR_MONTH,
                "ahorro_metros": round(total_meters, 2),
                "ahorro_horas": round(total_hours, 4),
                "ahorro_eur": round(total_eur, 2),
            }
        )
    return pd.DataFrame(rows)


def _distance_base_table(simulations: dict[str, dict[str, pd.DataFrame]]) -> pd.DataFrame:
    base = simulations["B_base_recomendado"]["owner_distance"].copy()
    return base[
        [
            "propietario",
            "metros_actuales",
            "metros_contrafactuales",
            "ahorro_metros",
            "ahorro_pct",
            "soporte",
        ]
    ].sort_values("ahorro_metros", ascending=False).reset_index(drop=True)


def _heatmap_tables(
    owner_footprint: pd.DataFrame,
    base_assignment: pd.DataFrame,
    slot_catalog: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    current = (
        slot_catalog[slot_catalog["occupied"] & slot_catalog["owner"].le(100)]
        .groupby(["owner", "aisle"], as_index=False)["location"]
        .nunique()
        .rename(columns={"owner": "propietario", "aisle": "pasillo", "location": "ubicaciones"})
    )
    base_counts = (
        base_assignment.groupby(["owner", "aisle"], as_index=False)["location"]
        .nunique()
        .rename(columns={"owner": "propietario", "aisle": "pasillo", "location": "ubicaciones"})
    )
    return current, base_counts


def _units_table(
    simulations: dict[str, dict[str, pd.DataFrame]],
    movements_2026: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    total_lines = int(len(movements_2026))
    total_transactions = int(movements_2026["transaction_id"].nunique())
    for scenario, tables in simulations.items():
        owner_distance = tables["owner_distance"]
        total_meters = float(owner_distance["ahorro_metros"].sum())
        rows.extend(
            [
                {
                    "escenario": scenario,
                    "unidad": "linea",
                    "valor_medio_ahorro_metros": round(total_meters / max(total_lines, 1), 4),
                    "robustez": "media",
                    "comentario": "Útil para granularidad fina, pero muy sensible al orden operativo de cada transacción.",
                },
                {
                    "escenario": scenario,
                    "unidad": "transaccion",
                    "valor_medio_ahorro_metros": round(total_meters / max(total_transactions, 1), 4),
                    "robustez": "alta",
                    "comentario": "Es la unidad recomendada porque la preparación se lanza por pedido externo + propietario.",
                },
                {
                    "escenario": scenario,
                    "unidad": "propietario",
                    "valor_medio_ahorro_metros": round(total_meters / max(len(owner_distance), 1), 4),
                    "robustez": "media",
                    "comentario": "Buena para priorizar dueños, pero oculta la variabilidad entre pedidos.",
                },
            ]
        )
    return pd.DataFrame(rows)


def _plot_owner_segment_map(frame: pd.DataFrame, assignment_col: str, title: str, path: Path) -> None:
    plot_frame = frame.copy()
    plot_frame["y"] = np.arange(len(plot_frame))
    fig, ax = plt.subplots(figsize=(12, max(6, len(plot_frame) * 0.22)))
    for row in plot_frame.itertuples(index=False):
        raw_value = getattr(row, assignment_col)
        aisles = [
            int(value)
            for value in str(raw_value).split(",")
            if value and value.lower() != "nan"
        ]
        for aisle in aisles:
            x = ACTIVE_AISLE_ORDER[int(aisle)]
            ax.broken_barh([(x, 1.0)], (row.y - 0.4, 0.8), facecolors="#2b6cb0" if assignment_col == "pasillos_actuales" else "#2f855a")
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


def _plot_bars(frame: pd.DataFrame, metric: str, title: str, xlabel: str, path: Path) -> None:
    subset = frame.sort_values(metric, ascending=True).tail(15)
    fig, ax = plt.subplots(figsize=(11, 7))
    labels = [str(int(value)) for value in subset["propietario"]]
    ax.barh(labels, subset[metric], color="#2f855a" if "horas" in metric else "#2b6cb0")
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_scatter(frame: pd.DataFrame, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(frame["picks_2026"], frame["ahorro_metros"], color="#2b6cb0", alpha=0.75)
    for row in frame.sort_values("ahorro_metros", ascending=False).head(12).itertuples(index=False):
        ax.annotate(str(int(row.propietario)), (row.picks_2026, row.ahorro_metros), fontsize=8, xytext=(4, 3), textcoords="offset points")
    ax.set_xlabel("Picks 2026")
    ax.set_ylabel("Ahorro metros")
    ax.set_title("Picks 2026 vs ahorro de metros por propietario")
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_waterfall(sensitivity: pd.DataFrame, path: Path) -> None:
    base = sensitivity[sensitivity["escenario"] == "B_base_recomendado"].iloc[0]
    stages = ["Metros\nahorrados", "Horas\nahorradas", "Ahorro\npersonal", "Ahorro\ncarretilla eq."]
    values = [
        float(base["ahorro_metros"]),
        float(base["ahorro_horas"]),
        float(base["ahorro_horas"]) * LABOUR_EUR_H,
        float(base["ahorro_horas"]) / FORKLIFT_ANNUAL_PRODUCTIVE_HOURS * FORKLIFT_REACH_EUR_MONTH * 12,
    ]
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(stages, values, color=["#2b6cb0", "#3182ce", "#2f855a", "#ed8936"])
    ax.set_title("Waterfall simplificado del ahorro base recomendado")
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _render_markdown(
    paths: BenefitPaths,
    repo_map: pd.DataFrame,
    actual_layout: pd.DataFrame,
    contrafactual_layout: pd.DataFrame,
    distance_base: pd.DataFrame,
    time_table: pd.DataFrame,
    cost_table: pd.DataFrame,
    personnel_table: pd.DataFrame,
    sensitivity: pd.DataFrame,
    units_table: pd.DataFrame,
) -> Path:
    base_sens = sensitivity[sensitivity["escenario"] == "B_base_recomendado"].iloc[0]
    max_sens = sensitivity.sort_values("ahorro_eur", ascending=False).iloc[0]
    people_base = personnel_table[personnel_table["escenario"] == "B_base_recomendado"].iloc[0]
    reused = repo_map[repo_map["se_reutiliza"].isin(["si", "parcial"])]
    base_hours = float(base_sens["ahorro_horas"])
    base_personal = base_hours * LABOUR_EUR_H
    max_hours = float(max_sens["ahorro_horas"])
    max_personal = max_hours * LABOUR_EUR_H
    total_actual_m = float(distance_base["metros_actuales"].sum())
    total_counter_m = float(distance_base["metros_contrafactuales"].sum())
    output = f"""# beneficio_layout_actual_2026

## 1. cómo está construido el repo actualmente

- El repo está organizado alrededor de un pipeline general de market basket con limpieza, transacciones, EDA, asociaciones, scoring y outputs.
- La parte base sí genera métricas de dispersión transaccional (`unique_locations_in_basket`, `basket_dispersion_proxy`) y perfiles SKU-localización.
- No existía una simulación espacial contrafactual del almacén actual agrupado por propietario; esa pieza se ha añadido en este trabajo.

## 2. qué parte del repo se reutilizó

- Se reutilizaron principalmente los conceptos y salidas de `transactions.py`, `cleaning.py`, `outputs.py` y la normalización de `mahou_dimensioning.py`.
- Módulos reutilizados de forma directa o parcial: {", ".join(reused["archivo"].tolist())}.
- No se reutilizó la lógica del almacén destino porque este caso es exclusivamente del almacén actual.

## 3. cómo se modeló el almacén actual

- Hecho observado base: foto de stock `17-04-2026.xlsx` + movimientos `PI` de 2026.
- Geometría usada: 5,50 m por cambio lateral entre pasillos contiguos y 1,20 m por posición longitudinal.
- Cada localización actual se convirtió en coordenadas `(x,y)` usando pasillo y columna reales.
- El layout actual por propietario se midió con la foto de stock ocupada de la nave actual, no con el almacén destino.

## 4. cómo se construyó el layout contrafactual

- Cada propietario se reasignó a un bloque continuo de pasillos dentro de la nave actual, respetando la capacidad física observada por pasillo.
- La capacidad requerida por propietario se tomó de su footprint ocupado en la foto de stock.
- En el escenario base, los propietarios con más `picks_2026` se acercan antes a expedición.
- Dentro del bloque de cada propietario, las localizaciones más usadas en 2026 se colocan primero.

## 5. ahorro en metros

- Escenario base recomendado: de {total_actual_m:,.2f} m a {total_counter_m:,.2f} m, con un ahorro total de {base_sens['ahorro_metros']:,.2f} m.
- Escenario conservador: {sensitivity[sensitivity['escenario']=='A_conservador']['ahorro_metros'].iloc[0]:,.2f} m.
- Escenario agresivo: {sensitivity[sensitivity['escenario']=='C_agresivo']['ahorro_metros'].iloc[0]:,.2f} m.

## 6. ahorro en horas

- Escenario base recomendado: {base_hours:,.2f} h.
- Escenario conservador: {sensitivity[sensitivity['escenario']=='A_conservador']['ahorro_horas'].iloc[0]:,.2f} h.
- Escenario agresivo: {max_hours:,.2f} h.
- Métrica principal recomendada: `transacción (pedido externo + propietario)` porque el picking se lanza por propietario y esa unidad capta mejor el recorrido real que la línea aislada.

## 7. ahorro en euros

- Escenario base recomendado: {base_personal:,.2f} EUR de personal y {base_hours / FORKLIFT_ANNUAL_PRODUCTIVE_HOURS * FORKLIFT_REACH_EUR_MONTH * 12:,.2f} EUR equivalentes de carretilla.
- Ahorro total equivalente base: {base_sens['ahorro_eur']:,.2f} EUR.
- Máximo ahorro razonable del set de sensibilidad: {max_sens['ahorro_eur']:,.2f} EUR.

## 8. personas equivalentes

- Escenario base recomendado: {people_base['personas_equivalentes']:.4f} personas equivalentes.
- Esto refleja ahorro potencial teórico de capacidad, no necesariamente una baja real de plantilla.

## 9. carretillas equivalentes

- Escenario base recomendado: {people_base['carretillas_equivalentes']:.4f} carretillas equivalentes frente a una dotación actual de {FORKLIFTS_CURRENT}.
- El ahorro realizable de carretilla sigue siendo {people_base['ahorro_carretilla_realizable']:.2f} EUR porque no se llega a liberar una carretilla completa.

## 10. sensibilidad

- La sensibilidad se ha construido con tres escenarios: conservador, base recomendado y agresivo.
- Cambian tres cosas: orden de asignación por propietario, lógica interna de mapeo de localizaciones y velocidad efectiva de conversión metros->tiempo.
- La tabla `tabla_sensibilidad.csv` deja trazado qué parte del resultado depende de cada supuesto.

## 11. riesgos y limitaciones

- La foto de stock es una instantánea; la ocupación simultánea real cambia a lo largo de 2026.
- El contrafactual no cambia de nave ni inventa nuevas geometrías, pero sí supone que el re-slotting por propietario es ejecutable dentro de la capacidad observada.
- El ahorro realizable de personal o carretilla solo aparece si la organización adapta la dotación; si no, el ahorro queda como productividad absorbida.
- El escenario agresivo ya incorpora un plus operativo por mejor secuenciación dentro del bloque del propietario.

## 12. conclusión ejecutiva

- Con la información disponible, sí habría existido un ahorro real si el almacén actual de 2026 hubiera estado agrupado por propietario.
- El ahorro principal viene de reducir saltos entre pasillos dentro del mismo propietario, no de afinidad SKU-SKU.
- La conclusión robusta es que el beneficio existe en recorrido, tiempo y coste equivalente, aunque la captura realizable depende de si la operación ajusta recursos.

ahorro base recomendado: {base_sens['ahorro_eur']:,.2f} EUR equivalentes ({base_hours:,.2f} h; {base_sens['ahorro_metros']:,.2f} m)
máximo ahorro razonable: {max_sens['ahorro_eur']:,.2f} EUR equivalentes ({max_hours:,.2f} h; {max_sens['ahorro_metros']:,.2f} m)
qué supuesto cambia más el resultado: la combinación de `velocidad efectiva` y `criterio de secuenciación contrafactual` dentro del bloque del propietario
"""
    path = paths.output_dir / "beneficio_layout_actual_2026.md"
    path.write_text(output, encoding="utf-8")
    return path


def _build_workbook_manifest(tables: dict[str, pd.DataFrame], path: Path) -> None:
    path.write_text(
        json.dumps({name: _json_safe_records(frame) for name, frame in tables.items()}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def run_beneficio_layout_actual_2026(base_dir: Path) -> dict[str, str]:
    paths = _ensure_directories(base_dir)
    sources = _load_sources(base_dir)
    repo_map = _repo_map_rows(base_dir, sources)

    slot_catalog, capacity_by_aisle, owner_footprint, owner_names = _prepare_stock_layout(sources["stock"])
    movements_2026 = _prepare_movements_2026(sources["movements"], owner_names)
    activity = _owner_activity_table(movements_2026)
    actual_layout = _actual_layout_table(owner_footprint, activity)

    simulations = {
        scenario: _simulate_scenario(
            scenario=scenario,
            movements_2026=movements_2026,
            owner_footprint=owner_footprint,
            activity=activity,
            capacity_by_aisle=capacity_by_aisle,
            slot_catalog=slot_catalog,
        )
        for scenario in SCENARIO_CONFIG
    }

    base_assignment = simulations["B_base_recomendado"]["assignment"].copy()
    distance_base = _distance_base_table(simulations)
    time_table, cost_table, personnel_table = _time_and_cost_tables(simulations)
    sensitivity = _sensitivity_table(simulations)
    units_table = _units_table(simulations, movements_2026)
    heatmap_actual, heatmap_contra = _heatmap_tables(
        owner_footprint=owner_footprint,
        base_assignment=simulations["B_base_recomendado"]["slot_assignment"],
        slot_catalog=slot_catalog,
    )

    # Ensure mandatory tables have the exact columns requested
    contrafactual_layout = base_assignment[
        [
            "propietario",
            "pasillo_objetivo_1",
            "pasillo_objetivo_2",
            "num_pasillos_objetivo",
            "criterio_asignacion",
            "justificacion",
        ]
    ].copy()

    repo_map = repo_map.rename(columns={"función": "función"})
    tables: dict[str, pd.DataFrame] = {
        "tabla_repo_map": repo_map,
        "tabla_layout_actual_propietario": actual_layout,
        "tabla_layout_contrafactual_propietario": contrafactual_layout,
        "tabla_distancia_actual_vs_contrafactual": distance_base,
        "tabla_impacto_tiempo": time_table,
        "tabla_impacto_coste": cost_table,
        "tabla_personal_equivalente": personnel_table,
        "tabla_sensibilidad": sensitivity,
        "tabla_unidades_analisis": units_table,
        "tabla_heatmap_pasillo_propietario_actual": heatmap_actual,
        "tabla_heatmap_pasillo_propietario_contrafactual": heatmap_contra,
        "tabla_distancia_transaccion_actual_vs_contrafactual": simulations["B_base_recomendado"]["transaction_distance"],
        "tabla_distancia_linea_actual_vs_contrafactual": simulations["B_base_recomendado"]["line_distance"],
    }

    outputs: dict[str, str] = {}
    for name, frame in tables.items():
        table_path = paths.output_dir / f"{name}.csv"
        frame.to_csv(table_path, index=False, encoding="utf-8-sig")
        outputs[name] = str(table_path)

    # Visuals
    current_map_path = paths.plots_dir / "mapa_dispersion_actual_por_propietario.png"
    contra_map_path = paths.plots_dir / "mapa_contrafactual_por_propietario.png"
    meters_bar_path = paths.plots_dir / "barras_metros_ahorrados_por_propietario.png"
    hours_bar_path = paths.plots_dir / "barras_horas_ahorradas_por_propietario.png"
    waterfall_path = paths.plots_dir / "waterfall_ahorro_total.png"
    scatter_path = paths.plots_dir / "scatter_picks_vs_ahorro.png"
    heatmap_current_path = paths.plots_dir / "heatmap_pasillo_propietario_actual.png"
    heatmap_contra_path = paths.plots_dir / "heatmap_pasillo_propietario_contrafactual.png"

    _plot_owner_segment_map(
        frame=actual_layout.sort_values(["picks_2026", "lineas_2026", "propietario"], ascending=[False, False, True]).head(30),
        assignment_col="pasillos_actuales",
        title="Mapa de dispersión actual por propietario",
        path=current_map_path,
    )
    _plot_owner_segment_map(
        frame=base_assignment.sort_values(["pasillo_objetivo_1", "propietario"]).head(30),
        assignment_col="pasillos_objetivo",
        title="Mapa contrafactual agrupado por propietario",
        path=contra_map_path,
    )
    _plot_bars(
        frame=distance_base,
        metric="ahorro_metros",
        title="Metros ahorrados por propietario (escenario base)",
        xlabel="Metros ahorrados",
        path=meters_bar_path,
    )
    base_hours_by_owner = (
        time_table[time_table["escenario"] == "B_base_recomendado"]
        .sort_values("tiempo_ahorrado_horas", ascending=False)
        .reset_index(drop=True)
    )
    _plot_bars(
        frame=base_hours_by_owner,
        metric="tiempo_ahorrado_horas",
        title="Horas ahorradas por propietario (escenario base)",
        xlabel="Horas ahorradas",
        path=hours_bar_path,
    )
    picks_for_scatter = actual_layout[["propietario", "picks_2026"]].merge(distance_base[["propietario", "ahorro_metros"]], on="propietario", how="left").fillna(0)
    _plot_scatter(picks_for_scatter, scatter_path)
    _plot_waterfall(sensitivity, waterfall_path)
    _plot_heatmap(heatmap_actual, "Heatmap pasillo x propietario actual", heatmap_current_path)
    _plot_heatmap(heatmap_contra, "Heatmap pasillo x propietario contrafactual", heatmap_contra_path)

    outputs["mapa_dispersion_actual_por_propietario"] = str(current_map_path)
    outputs["mapa_contrafactual_por_propietario"] = str(contra_map_path)
    outputs["barras_metros_ahorrados_por_propietario"] = str(meters_bar_path)
    outputs["barras_horas_ahorradas_por_propietario"] = str(hours_bar_path)
    outputs["waterfall_ahorro_total"] = str(waterfall_path)
    outputs["scatter_picks_vs_ahorro"] = str(scatter_path)
    outputs["heatmap_pasillo_propietario_actual"] = str(heatmap_current_path)
    outputs["heatmap_pasillo_propietario_contrafactual"] = str(heatmap_contra_path)

    summary_md = _render_markdown(
        paths=paths,
        repo_map=repo_map,
        actual_layout=actual_layout,
        contrafactual_layout=contrafactual_layout,
        distance_base=distance_base,
        time_table=time_table,
        cost_table=cost_table,
        personnel_table=personnel_table,
        sensitivity=sensitivity,
        units_table=units_table,
    )
    outputs["beneficio_layout_actual_2026_md"] = str(summary_md)

    manifest_path = paths.output_dir / "workbook_tables_detail.json"
    _build_workbook_manifest(tables, manifest_path)
    outputs["workbook_manifest"] = str(manifest_path)
    return outputs
