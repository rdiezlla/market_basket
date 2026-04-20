from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from .mahou_dimensioning import _normalize_dataframe_columns, _prepare_movements


HISTORY_START = pd.Timestamp("2026-01-01")
HISTORY_END = pd.Timestamp("2026-04-20 23:59:59")
ANNUALIZATION_FACTOR = 365 / 110
LABOUR_COST_EUR_H = 22.0
RETRACT_AISLE_WIDTH_M = 3.0
ARTICULATED_AISLE_WIDTH_M = 2.0
RACK_DEPTH_M = 1.10
RETRACT_TRAVEL_SPEED_KMH = 11.0
ARTICULATED_SLOWDOWN_RATIO = 0.12
TYPICAL_PICK_LINE_CYCLE_SEC = 255.0
EXTERNAL_EURO_PALLET_RATE_EUR_MONTH = 6.20
ORIGIN_AISLE_SEQUENCE = [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 20]
MIXED_COMPROMISE_AISLES = [11, 12]
REAR_ARTICULATED_SENSITIVITY = {
    1: [12],
    2: [11, 12],
    3: [10, 11, 12],
    4: [9, 10, 11, 12],
}
EUR_GBP_REFERENCE = 0.86993
GBP_TO_EUR = 1 / EUR_GBP_REFERENCE

SOURCE_COST_REFERENCES = [
    {
        "equipo": "reach_truck",
        "concepto": "alquiler_mensual",
        "valor_moneda_origen": 520.0,
        "moneda_origen": "GBP",
        "valor_eur": round(520.0 * GBP_TO_EUR, 2),
        "unidad": "GBP/mes",
        "fuente": "Multy Lift Caterpillar NR20NH (3919)",
        "url": "https://www.multylift.co.uk/caterpillar-nr20nh-3919/",
        "soporte": "Reach truck usado ofertado con hire mensual.",
    },
    {
        "equipo": "reach_truck",
        "concepto": "compra_usada",
        "valor_moneda_origen": 8500.0,
        "moneda_origen": "GBP",
        "valor_eur": round(8500.0 * GBP_TO_EUR, 2),
        "unidad": "GBP",
        "fuente": "Multy Lift Caterpillar NR20NH (3919)",
        "url": "https://www.multylift.co.uk/caterpillar-nr20nh-3919/",
        "soporte": "Precio contado reach truck usado.",
    },
    {
        "equipo": "articulated_truck",
        "concepto": "alquiler_mensual",
        "valor_moneda_origen": 1018.0,
        "moneda_origen": "GBP",
        "valor_eur": round(1018.0 * GBP_TO_EUR, 2),
        "unidad": "GBP/mes",
        "fuente": "Multy Lift Aisle Master Forklift Hire",
        "url": "https://www.multylift.co.uk/aisle-master-forklift-hire/",
        "soporte": "Aisle Master articulada/VNA en hire mensual.",
    },
    {
        "equipo": "articulated_truck",
        "concepto": "compra_usada",
        "valor_moneda_origen": 14450.0,
        "moneda_origen": "GBP",
        "valor_eur": round(14450.0 * GBP_TO_EUR, 2),
        "unidad": "GBP",
        "fuente": "Trucks Direct UK Aisle Master sold archive",
        "url": "https://www.trucksdirectuk.co.uk/c/aisle-master-sold-archive",
        "soporte": "Aisle Master articulada usada vendida.",
    },
    {
        "equipo": "articulated_truck",
        "concepto": "compra_nueva_desde",
        "valor_moneda_origen": 29899.0,
        "moneda_origen": "GBP",
        "valor_eur": round(29899.0 * GBP_TO_EUR, 2),
        "unidad": "GBP",
        "fuente": "Trucks Direct UK Aisle Master / articulated",
        "url": "https://www.trucksdirectuk.co.uk/c/new-forklifts/aisle-master-forklift",
        "soporte": "Articulada nueva desde precio catálogo.",
    },
]

SOURCE_REFERENCE_ROWS = [
    {
        "tema": "ancho_pasillo_general",
        "dato": "La maniobrabilidad depende del ancho mínimo de pasillo del equipo.",
        "fuente": "Toyota Material Handling Europe",
        "url": "https://toyota-forklifts.eu/guides/aisle-width-guide/",
    },
    {
        "tema": "reach_speed",
        "dato": "Reach truck Crown ESR 1000: 11 km/h con carga / sin carga.",
        "fuente": "Crown ESR 1000 specification",
        "url": "https://www.crown.com/content/dam/crown/pdfs/apac/specs/spec-sheet-esr-1000.pdf",
    },
    {
        "tema": "articulated_speed",
        "dato": "Aisle Master eléctrico: 16 km/h nominales y 1.5-2.5 t de capacidad.",
        "fuente": "Aisle Master electric product page",
        "url": "https://aisle-master.com/de/produkte/ac-elektrischer-aisle-master/",
    },
    {
        "tema": "articulated_capacity_claim",
        "dato": "Aisle Master reclama 30% más almacenamiento que reach truck.",
        "fuente": "Aisle Master brochure",
        "url": "https://www.multylift.co.uk/wp-content/uploads/2021/03/Aisle-Master-Articulated-Forklifts-Brochure.pdf",
    },
    {
        "tema": "storage_rate_external",
        "dato": "Tarifa pública de almacenaje: euro pallet 6.20 €/mes; americano 8.20 €/mes.",
        "fuente": "FEPOPA rate card",
        "url": "https://fepopa.com/wp-content/uploads/2024/05/Amacenaje-y-manipulacion-FEPOPA-2024.pdf",
    },
    {
        "tema": "fx_reference",
        "dato": "ECB: 1 EUR = 0.86993 GBP",
        "fuente": "ECB euro reference rates",
        "url": "https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html",
    },
]


def _json_safe_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    export = frame.copy()
    for column in export.columns:
        if pd.api.types.is_datetime64_any_dtype(export[column]):
            export[column] = export[column].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
    return json.loads(export.to_json(orient="records", date_format="iso"))


def _load_inputs(base_dir: Path) -> dict[str, pd.DataFrame]:
    movements = _normalize_dataframe_columns(pd.read_excel(base_dir / "movimientos.xlsx"))
    demand = pd.read_csv(base_dir / "output" / "mahou_codex" / "csv" / "tabla_demanda_propietario.csv")
    layout = pd.read_csv(
        base_dir / "output" / "mahou_codex_rotacion_2026" / "detail" / "tabla_destino_propietario_rangos_resumen.csv"
    )
    owner_abc = pd.read_csv(
        base_dir / "output" / "mahou_codex_rotacion_2026" / "detail" / "tabla_owner_picking_abc_2026.csv"
    )
    return {
        "movements": movements,
        "demand": demand,
        "layout": layout,
        "owner_abc": owner_abc,
    }


def _prepare_pick_lines(movements: pd.DataFrame) -> pd.DataFrame:
    prepared = _prepare_movements(movements).copy()
    prepared = prepared[
        prepared["fecha_finalizacion"].between(HISTORY_START, HISTORY_END, inclusive="both")
    ].copy()
    prepared = prepared[prepared["pasillo_origen"].isin(ORIGIN_AISLE_SEQUENCE)].copy()
    rank_map = {aisle: index + 1 for index, aisle in enumerate(ORIGIN_AISLE_SEQUENCE)}
    prepared["source_rank"] = prepared["pasillo_origen"].map(rank_map)
    prepared["duracion_seg"] = (
        prepared["fecha_finalizacion"] - prepared["fecha_inicio"]
    ).dt.total_seconds()
    return prepared


def _layout_allocations(layout: pd.DataFrame, demand: pd.DataFrame, owner_abc: pd.DataFrame) -> pd.DataFrame:
    alloc = (
        layout.groupby(["propietario", "pasillo_destino", "owner_name"], as_index=False)["posiciones_totales"]
        .sum()
        .copy()
    )
    alloc["propietario"] = pd.to_numeric(alloc["propietario"], errors="coerce")
    demand = demand.copy()
    demand["propietario"] = pd.to_numeric(demand["propietario"], errors="coerce")
    owner_abc = owner_abc.copy()
    owner_abc["owner"] = pd.to_numeric(owner_abc["owner"], errors="coerce")
    totals = alloc.groupby("propietario", as_index=False)["posiciones_totales"].sum().rename(
        columns={"posiciones_totales": "total_posiciones"}
    )
    alloc = alloc.merge(totals, on="propietario", how="left")
    alloc["peso_posiciones"] = alloc["posiciones_totales"] / alloc["total_posiciones"]
    alloc = alloc.merge(
        demand[["propietario", "demanda_actual_eu", "demanda_actual_am", "demanda_actual_tr"]],
        on="propietario",
        how="left",
    )
    alloc = alloc.merge(
        owner_abc[["owner", "picking_lineas_2026", "picking_transacciones_2026", "picking_unidades_2026", "abc_picking_2026"]],
        left_on="propietario",
        right_on="owner",
        how="left",
    )
    for column in [
        "demanda_actual_eu",
        "demanda_actual_am",
        "demanda_actual_tr",
        "picking_lineas_2026",
        "picking_transacciones_2026",
        "picking_unidades_2026",
    ]:
        alloc[column] = alloc[column].fillna(0.0)
    alloc["picking_lineas_asignadas_2026"] = alloc["peso_posiciones"] * alloc["picking_lineas_2026"]
    alloc["picking_transacciones_asignadas_2026"] = (
        alloc["peso_posiciones"] * alloc["picking_transacciones_2026"]
    )
    alloc["eu_asignado"] = alloc["peso_posiciones"] * alloc["demanda_actual_eu"]
    alloc["am_asignado"] = alloc["peso_posiciones"] * alloc["demanda_actual_am"]
    alloc["tr_asignado"] = alloc["peso_posiciones"] * alloc["demanda_actual_tr"]
    alloc["am_tr_asignado"] = alloc["am_asignado"] + alloc["tr_asignado"]
    return alloc


def _owner_productivity_table(picks: pd.DataFrame, alloc: pd.DataFrame) -> pd.DataFrame:
    current = (
        picks.groupby("propietario")
        .agg(
            picking_lineas_2026=("transaction_id", "count"),
            source_rank_actual=("source_rank", "mean"),
            pasillos_actuales=("pasillo_origen", lambda s: ",".join(str(int(v)) for v in sorted(s.dropna().unique()))),
            n_pasillos_actual=("pasillo_origen", lambda s: int(s.dropna().nunique())),
            span_pasillos_actual=("pasillo_origen", lambda s: float(s.max() - s.min()) if len(s.dropna()) else 0.0),
        )
        .reset_index()
    )
    destination = (
        alloc.groupby("propietario")
        .apply(
            lambda frame: pd.Series(
                {
                    "owner_name": frame["owner_name"].dropna().iloc[0] if frame["owner_name"].notna().any() else "",
                    "dest_rank_rotacion_2026": float((frame["pasillo_destino"] * frame["peso_posiciones"]).sum()),
                    "pasillos_destino": ",".join(str(int(v)) for v in sorted(frame["pasillo_destino"].unique())),
                    "n_pasillos_destino": int(frame["pasillo_destino"].nunique()),
                    "span_pasillos_destino": float(frame["pasillo_destino"].max() - frame["pasillo_destino"].min()),
                    "abc_2026": frame["abc_picking_2026"].dropna().iloc[0] if frame["abc_picking_2026"].notna().any() else "SIN_ABC",
                }
            ),
            include_groups=False,
        )
        .reset_index()
    )
    merged = current.merge(destination, on="propietario", how="left")
    merged["delta_pasillos"] = merged["source_rank_actual"] - merged["dest_rank_rotacion_2026"]
    seconds_per_rank = 2 * (RETRACT_AISLE_WIDTH_M + RACK_DEPTH_M) / (RETRACT_TRAVEL_SPEED_KMH / 3.6)
    merged["segundos_paseo_ahorrados_por_linea"] = merged["delta_pasillos"] * seconds_per_rank
    merged["horas_paseo_ahorradas_ytd"] = (
        merged["picking_lineas_2026"] * merged["segundos_paseo_ahorrados_por_linea"] / 3600
    )
    merged["horas_paseo_ahorradas_anualizadas"] = merged["horas_paseo_ahorradas_ytd"] * ANNUALIZATION_FACTOR
    merged["ahorro_laboral_anual_eur"] = merged["horas_paseo_ahorradas_anualizadas"] * LABOUR_COST_EUR_H
    merged = merged.sort_values(
        ["ahorro_laboral_anual_eur", "picking_lineas_2026"],
        ascending=[False, False],
    ).reset_index(drop=True)
    return merged


def _aisle_mix_table(alloc: pd.DataFrame) -> pd.DataFrame:
    by_aisle = (
        alloc.groupby("pasillo_destino")
        .agg(
            propietarios=("propietario", "nunique"),
            posiciones_asignadas=("posiciones_totales", "sum"),
            picking_lineas_asignadas_2026=("picking_lineas_asignadas_2026", "sum"),
            picking_transacciones_asignadas_2026=("picking_transacciones_asignadas_2026", "sum"),
            eu_asignado=("eu_asignado", "sum"),
            am_asignado=("am_asignado", "sum"),
            tr_asignado=("tr_asignado", "sum"),
            am_tr_asignado=("am_tr_asignado", "sum"),
        )
        .reset_index()
    )
    by_aisle["truck_type_compromise"] = by_aisle["pasillo_destino"].apply(
        lambda aisle: "articulada" if aisle in MIXED_COMPROMISE_AISLES else "retractil"
    )
    return by_aisle


def _scenario_table(owner_table: pd.DataFrame, by_aisle: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    current_proxy_hours_ytd = (
        owner_table["picking_lineas_2026"] * owner_table["source_rank_actual"]
    ).sum() * (2 * (RETRACT_AISLE_WIDTH_M + RACK_DEPTH_M) / (RETRACT_TRAVEL_SPEED_KMH / 3.6)) / 3600
    current_proxy_hours_annual = current_proxy_hours_ytd * ANNUALIZATION_FACTOR
    retract_proxy_hours_annual = current_proxy_hours_annual - owner_table["horas_paseo_ahorradas_anualizadas"].sum()

    rows = [
        {
            "escenario": "actual_2026_origen",
            "pasillos_articulados": "",
            "horas_paseo_proxy_anual": round(current_proxy_hours_annual, 2),
            "coste_paseo_proxy_anual_eur": round(current_proxy_hours_annual * LABOUR_COST_EUR_H, 2),
            "coste_equipo_anual_eur_por_maquina": 0.0,
            "capacidad_extra_posiciones_equivalentes": 0.0,
            "valor_capacidad_si_se_ocupa_100_pct_eur_anual": 0.0,
            "am_tr_incompatible_en_altura": 0.0,
            "neto_vs_retractil_antes_de_amtr_eur_anual": 0.0,
            "flexibilidad_neta_posiciones_equivalentes": 0.0,
            "comentario": "Base observada 2026; sin hipótesis nueva de carretilla.",
        },
        {
            "escenario": "destino_12_pasillos_retractil",
            "pasillos_articulados": "",
            "horas_paseo_proxy_anual": round(retract_proxy_hours_annual, 2),
            "coste_paseo_proxy_anual_eur": round(retract_proxy_hours_annual * LABOUR_COST_EUR_H, 2),
            "coste_equipo_anual_eur_por_maquina": round(
                SOURCE_COST_REFERENCES[0]["valor_eur"] * 12, 2
            ),
            "capacidad_extra_posiciones_equivalentes": 0.0,
            "valor_capacidad_si_se_ocupa_100_pct_eur_anual": 0.0,
            "am_tr_incompatible_en_altura": 0.0,
            "neto_vs_retractil_antes_de_amtr_eur_anual": 0.0,
            "flexibilidad_neta_posiciones_equivalentes": 0.0,
            "comentario": "Escenario recomendado base: toda la nave servida con reach/retráctil.",
        },
    ]

    sensitivity_rows = []
    annual_equipment_premium = (
        SOURCE_COST_REFERENCES[2]["valor_eur"] - SOURCE_COST_REFERENCES[0]["valor_eur"]
    ) * 12
    for articulated_count, aisles in REAR_ARTICULATED_SENSITIVITY.items():
        aisle_frame = by_aisle[by_aisle["pasillo_destino"].isin(aisles)].copy()
        extra_positions = articulated_count * 108 * (
            ((RETRACT_AISLE_WIDTH_M + RACK_DEPTH_M) / (ARTICULATED_AISLE_WIDTH_M + RACK_DEPTH_M)) - 1
        )
        penalty_hours_annual = (
            aisle_frame["picking_lineas_asignadas_2026"].sum()
            * TYPICAL_PICK_LINE_CYCLE_SEC
            * ARTICULATED_SLOWDOWN_RATIO
            / 3600
            * ANNUALIZATION_FACTOR
        )
        penalty_eur_annual = penalty_hours_annual * LABOUR_COST_EUR_H
        storage_value_annual = extra_positions * EXTERNAL_EURO_PALLET_RATE_EUR_MONTH * 12
        amtr_incompatible = aisle_frame["am_tr_asignado"].sum()
        row = {
            "articulated_count": articulated_count,
            "pasillos_articulados": ",".join(str(aisle) for aisle in aisles),
            "picking_lineas_en_pasillos_articulados_2026": round(aisle_frame["picking_lineas_asignadas_2026"].sum(), 2),
            "am_tr_incompatible_en_altura": round(amtr_incompatible, 2),
            "capacidad_extra_posiciones_equivalentes": round(extra_positions, 2),
            "valor_capacidad_si_se_ocupa_100_pct_eur_anual": round(storage_value_annual, 2),
            "penalizacion_laboral_eur_anual": round(penalty_eur_annual, 2),
            "prima_equipo_eur_anual_por_maquina": round(annual_equipment_premium, 2),
            "neto_antes_de_amtr_eur_anual": round(storage_value_annual - penalty_eur_annual - annual_equipment_premium, 2),
            "flexibilidad_neta_posiciones_equivalentes": round(extra_positions - amtr_incompatible, 2),
        }
        sensitivity_rows.append(row)
        if aisles == MIXED_COMPROMISE_AISLES:
            rows.append(
                {
                    "escenario": "destino_mixto_compromiso_11_12_articulada",
                    "pasillos_articulados": row["pasillos_articulados"],
                    "horas_paseo_proxy_anual": round(retract_proxy_hours_annual + penalty_hours_annual, 2),
                    "coste_paseo_proxy_anual_eur": round(
                        (retract_proxy_hours_annual + penalty_hours_annual) * LABOUR_COST_EUR_H, 2
                    ),
                    "coste_equipo_anual_eur_por_maquina": round(
                        SOURCE_COST_REFERENCES[2]["valor_eur"] * 12, 2
                    ),
                    "capacidad_extra_posiciones_equivalentes": row["capacidad_extra_posiciones_equivalentes"],
                    "valor_capacidad_si_se_ocupa_100_pct_eur_anual": row[
                        "valor_capacidad_si_se_ocupa_100_pct_eur_anual"
                    ],
                    "am_tr_incompatible_en_altura": row["am_tr_incompatible_en_altura"],
                    "neto_vs_retractil_antes_de_amtr_eur_anual": row["neto_antes_de_amtr_eur_anual"],
                    "flexibilidad_neta_posiciones_equivalentes": row["flexibilidad_neta_posiciones_equivalentes"],
                    "comentario": (
                        "Compromiso razonable si se quiere probar articulada atrás, "
                        "pero ya pierde flexibilidad por AM/TR."
                    ),
                }
            )

    scenarios = pd.DataFrame(rows)
    sensitivity = pd.DataFrame(sensitivity_rows)
    return scenarios, sensitivity


def _uncovered_owner_table(
    picks: pd.DataFrame,
    owner_table: pd.DataFrame,
    owner_abc: pd.DataFrame,
) -> pd.DataFrame:
    uncovered = owner_table[owner_table["dest_rank_rotacion_2026"].isna()].copy()
    if uncovered.empty:
        return pd.DataFrame(
            columns=[
                "propietario",
                "owner_name",
                "picking_lineas_2026",
                "picking_transacciones_2026",
                "source_rank_actual",
                "pasillos_actuales",
            ]
        )
    owner_abc = owner_abc.copy()
    owner_abc["owner"] = pd.to_numeric(owner_abc["owner"], errors="coerce")
    uncovered = uncovered.merge(
        owner_abc[["owner", "owner_name", "picking_transacciones_2026"]],
        left_on="propietario",
        right_on="owner",
        how="left",
    )
    uncovered["owner_name"] = uncovered["owner_name_y"].combine_first(uncovered["owner_name_x"])
    uncovered = uncovered[
        [
            "propietario",
            "owner_name",
            "picking_lineas_2026",
            "picking_transacciones_2026",
            "source_rank_actual",
            "pasillos_actuales",
        ]
    ].sort_values("picking_lineas_2026", ascending=False)
    return uncovered.reset_index(drop=True)


def _assumptions_table() -> pd.DataFrame:
    rows = [
        {
            "supuesto": "periodo_analizado",
            "valor": f"{HISTORY_START.date().isoformat()} a {HISTORY_END.date().isoformat()}",
            "unidad": "fecha",
            "fuente": "movimientos.xlsx",
            "comentario": "Rotación 2026 usada como necesidad más representativa.",
        },
        {
            "supuesto": "coste_personal",
            "valor": LABOUR_COST_EUR_H,
            "unidad": "EUR/h",
            "fuente": "usuario",
            "comentario": "Coste horario aportado por negocio.",
        },
        {
            "supuesto": "ancho_pasillo_retractil",
            "valor": RETRACT_AISLE_WIDTH_M,
            "unidad": "m",
            "fuente": "usuario",
            "comentario": "Escenario todo retráctil.",
        },
        {
            "supuesto": "ancho_pasillo_articulada",
            "valor": ARTICULATED_AISLE_WIDTH_M,
            "unidad": "m",
            "fuente": "usuario",
            "comentario": "Escenario pasillo estrecho.",
        },
        {
            "supuesto": "profundidad_rack_modelada",
            "valor": RACK_DEPTH_M,
            "unidad": "m",
            "fuente": "proxy geométrico",
            "comentario": "Usada para convertir ahorro de ancho en posiciones equivalentes.",
        },
        {
            "supuesto": "velocidad_retractil_modelada",
            "valor": RETRACT_TRAVEL_SPEED_KMH,
            "unidad": "km/h",
            "fuente": "Crown ESR 1000",
            "comentario": "Velocidad de desplazamiento de referencia.",
        },
        {
            "supuesto": "penalizacion_productividad_articulada",
            "valor": ARTICULATED_SLOWDOWN_RATIO,
            "unidad": "ratio",
            "fuente": "supuesto operativo",
            "comentario": "Penalización deliberadamente conservadora aplicada solo a líneas en pasillos articulados.",
        },
        {
            "supuesto": "ciclo_pick_tipico",
            "valor": TYPICAL_PICK_LINE_CYCLE_SEC,
            "unidad": "seg/linea",
            "fuente": "mediana movimientos 2026",
            "comentario": "Usada para penalizar articulada por menor productividad.",
        },
        {
            "supuesto": "tarifa_externa_euro_palet",
            "valor": EXTERNAL_EURO_PALLET_RATE_EUR_MONTH,
            "unidad": "EUR/palet/mes",
            "fuente": "FEPOPA",
            "comentario": "Valor de capacidad solo si la nueva posición realmente sustituye externo.",
        },
        {
            "supuesto": "fx_eur_gbp",
            "valor": EUR_GBP_REFERENCE,
            "unidad": "GBP por EUR",
            "fuente": "ECB",
            "comentario": "Cambio usado para homogeneizar costes de equipo publicados en GBP.",
        },
    ]
    return pd.DataFrame(rows)


def _references_table() -> pd.DataFrame:
    return pd.DataFrame(SOURCE_REFERENCE_ROWS)


def _cost_table() -> pd.DataFrame:
    return pd.DataFrame(SOURCE_COST_REFERENCES)


def _render_charts(
    detail_dir: Path,
    scenarios: pd.DataFrame,
    sensitivity: pd.DataFrame,
    owner_table: pd.DataFrame,
    by_aisle: pd.DataFrame,
) -> dict[str, str]:
    charts_dir = detail_dir / "graficos_rentabilidad"
    charts_dir.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, str] = {}

    plt.style.use("seaborn-v0_8-whitegrid")

    # Scenario comparison
    fig, ax = plt.subplots(figsize=(11, 6))
    scenario_plot = scenarios[scenarios["escenario"] != "actual_2026_origen"].copy()
    x = range(len(scenario_plot))
    ax.bar(x, scenario_plot["coste_paseo_proxy_anual_eur"], label="Coste paseo proxy", color="#2b6cb0")
    ax.bar(x, scenario_plot["coste_equipo_anual_eur_por_maquina"], label="Coste equipo/año por máquina", color="#ed8936", alpha=0.75)
    ax.bar(
        x,
        scenario_plot["valor_capacidad_si_se_ocupa_100_pct_eur_anual"],
        label="Valor capacidad si se usa",
        color="#38a169",
        alpha=0.65,
    )
    ax.set_xticks(list(x))
    ax.set_xticklabels(
        [value.replace("destino_", "").replace("_", "\n") for value in scenario_plot["escenario"]],
        rotation=0,
    )
    ax.set_ylabel("EUR/año")
    ax.set_title("Comparativa económica de escenarios 2026")
    ax.legend()
    fig.tight_layout()
    scenario_path = charts_dir / "comparativa_escenarios_rentabilidad.png"
    fig.savefig(scenario_path, dpi=200)
    plt.close(fig)
    outputs["comparativa_escenarios_rentabilidad"] = str(scenario_path)

    # Sensitivity articulated aisles
    fig, ax1 = plt.subplots(figsize=(11, 6))
    ax1.plot(
        sensitivity["articulated_count"],
        sensitivity["capacidad_extra_posiciones_equivalentes"],
        marker="o",
        linewidth=2.2,
        color="#2f855a",
        label="Capacidad extra",
    )
    ax1.plot(
        sensitivity["articulated_count"],
        sensitivity["am_tr_incompatible_en_altura"],
        marker="o",
        linewidth=2.2,
        color="#c53030",
        label="AM/TR incompatibles",
    )
    ax1.set_xlabel("Número de pasillos articulados al fondo")
    ax1.set_ylabel("Posiciones equivalentes")
    ax2 = ax1.twinx()
    ax2.bar(
        sensitivity["articulated_count"],
        sensitivity["neto_antes_de_amtr_eur_anual"],
        width=0.28,
        color="#dd6b20",
        alpha=0.35,
        label="Neto antes de AM/TR",
    )
    ax2.set_ylabel("EUR/año")
    ax1.set_title("Sensibilidad: pasillos articulados al fondo")
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
    fig.tight_layout()
    sensitivity_path = charts_dir / "sensibilidad_pasillos_articulados.png"
    fig.savefig(sensitivity_path, dpi=200)
    plt.close(fig)
    outputs["sensibilidad_pasillos_articulados"] = str(sensitivity_path)

    # Top owners savings
    top = owner_table.head(12).sort_values("ahorro_laboral_anual_eur", ascending=True)
    fig, ax = plt.subplots(figsize=(10, 7))
    labels = [f"{int(row.propietario)} {row.owner_name}" for row in top.itertuples(index=False)]
    ax.barh(labels, top["ahorro_laboral_anual_eur"], color="#3182ce")
    ax.set_xlabel("EUR/año")
    ax.set_title("Top propietarios con más ahorro de paseo en 2026")
    fig.tight_layout()
    owner_path = charts_dir / "top_propietarios_ahorro_2026.png"
    fig.savefig(owner_path, dpi=200)
    plt.close(fig)
    outputs["top_propietarios_ahorro_2026"] = str(owner_path)

    # Aisle mix
    fig, ax1 = plt.subplots(figsize=(11, 6))
    ax1.bar(by_aisle["pasillo_destino"] - 0.2, by_aisle["picking_lineas_asignadas_2026"], width=0.38, color="#4299e1", label="Líneas 2026 asignadas")
    ax1.set_xlabel("Pasillo destino")
    ax1.set_ylabel("Líneas de picking 2026")
    ax2 = ax1.twinx()
    ax2.bar(by_aisle["pasillo_destino"] + 0.2, by_aisle["am_tr_asignado"], width=0.38, color="#e53e3e", alpha=0.45, label="AM/TR asignado")
    ax2.set_ylabel("AM + TR asignado")
    ax1.set_title("Rotación 2026 vs restricción AM/TR por pasillo destino")
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right")
    fig.tight_layout()
    aisle_path = charts_dir / "pasillos_rotacion_amtr_2026.png"
    fig.savefig(aisle_path, dpi=200)
    plt.close(fig)
    outputs["pasillos_rotacion_amtr_2026"] = str(aisle_path)
    return outputs


def _render_markdown(
    detail_dir: Path,
    scenarios: pd.DataFrame,
    sensitivity: pd.DataFrame,
    owner_table: pd.DataFrame,
    uncovered: pd.DataFrame,
) -> Path:
    retract = scenarios.loc[scenarios["escenario"] == "destino_12_pasillos_retractil"].iloc[0]
    mixed = scenarios.loc[scenarios["escenario"] == "destino_mixto_compromiso_11_12_articulada"].iloc[0]
    best_paper = sensitivity.sort_values("neto_antes_de_amtr_eur_anual", ascending=False).iloc[0]
    weighted_current_aisles = (
        (owner_table["n_pasillos_actual"] * owner_table["picking_lineas_2026"]).sum()
        / owner_table["picking_lineas_2026"].sum()
    )
    weighted_new_aisles = (
        (owner_table["n_pasillos_destino"] * owner_table["picking_lineas_2026"]).sum()
        / owner_table["picking_lineas_2026"].sum()
    )
    weighted_current_span = (
        (owner_table["span_pasillos_actual"] * owner_table["picking_lineas_2026"]).sum()
        / owner_table["picking_lineas_2026"].sum()
    )
    weighted_new_span = (
        (owner_table["span_pasillos_destino"] * owner_table["picking_lineas_2026"]).sum()
        / owner_table["picking_lineas_2026"].sum()
    )
    top_owner = owner_table.iloc[0]
    uncovered_lines = float(uncovered["picking_lineas_2026"].sum()) if not uncovered.empty else 0.0
    uncovered_names = (
        ", ".join(f"{int(row.propietario)} {row.owner_name}" for row in uncovered.itertuples(index=False))
        if not uncovered.empty
        else "ninguno"
    )
    text = f"""# Rentabilidad 2026 Mahou

## Qué se ha comparado

- Base operativa: layout `rotacion_2026` ya validado.
- Escenario A: 12 pasillos a retráctil.
- Escenario B: escenario mixto de compromiso con pasillos {mixed['pasillos_articulados']} a articulada.
- Sensibilidad adicional: convertir de 1 a 4 pasillos finales para ver cuándo empieza a compensar solo por capacidad.

## Hallazgos clave

- El layout `rotacion_2026` reduce el paseo proxy anual de {scenarios.iloc[0]['horas_paseo_proxy_anual']:.2f} h a {retract['horas_paseo_proxy_anual']:.2f} h.
- Eso equivale a {owner_table['horas_paseo_ahorradas_anualizadas'].sum():.2f} h/año y {owner_table['ahorro_laboral_anual_eur'].sum():.2f} EUR/año de ahorro laboral directo solo por cercanía a expedición.
- La concentración mejora de {weighted_current_aisles:.2f} pasillos medios por propietario a {weighted_new_aisles:.2f}, y el span medio baja de {weighted_current_span:.2f} a {weighted_new_span:.2f} pasillos.
- El propietario que más ahorro captura es {int(top_owner['propietario'])} {top_owner['owner_name']}, con {top_owner['ahorro_laboral_anual_eur']:.2f} EUR/año proxy.
- Quedan fuera del layout de detalle {uncovered_lines:.0f} líneas 2026 de los propietarios: {uncovered_names}. No se les ha imputado ahorro artificial.

## Escenario A: todo retráctil

- Coste equipo de referencia por máquina: {retract['coste_equipo_anual_eur_por_maquina']:.2f} EUR/año.
- No crea incompatibilidades AM/TR en altura.
- Mantiene la lógica operativa de poner delante lo que más rota ahora.
- Es la opción más limpia para ahorrar tiempo sin reabrir el problema tipológico.

## Escenario B: mixto 11-12 articulada

- Prima de equipo frente a reach: {(SOURCE_COST_REFERENCES[2]['valor_eur'] - SOURCE_COST_REFERENCES[0]['valor_eur']) * 12:.2f} EUR/año por máquina.
- Capacidad extra teórica: {mixed['capacidad_extra_posiciones_equivalentes']:.2f} posiciones equivalentes.
- Valor anual de esa capacidad si se llenase al 100% sustituyendo externo tipo EUR: {mixed['valor_capacidad_si_se_ocupa_100_pct_eur_anual']:.2f} EUR/año.
- Pero deja {mixed['am_tr_incompatible_en_altura']:.2f} posiciones equivalentes AM/TR sin encaje natural en altura.
- Resultado: la flexibilidad neta cae a {mixed['flexibilidad_neta_posiciones_equivalentes']:.2f} posiciones equivalentes.

## Lectura de negocio

- Si solo miras almacenamiento teórico, abrir más pasillos articulados mejora la foto económica.
- El mejor caso “sobre el papel” es {int(best_paper['articulated_count'])} pasillos articulados ({best_paper['pasillos_articulados']}), con {best_paper['neto_antes_de_amtr_eur_anual']:.2f} EUR/año antes de castigar AM/TR.
- Pero ese mismo caso deja {best_paper['am_tr_incompatible_en_altura']:.2f} posiciones AM/TR problemáticas y destruye {abs(best_paper['flexibilidad_neta_posiciones_equivalentes']):.2f} posiciones equivalentes netas de flexibilidad.

## Recomendación

- Recomendación operativa y económica: **todo retráctil**.
- Motivo: el ahorro por cercanía y concentración ya aparece con `rotacion_2026`, mientras que la articulada solo empieza a defenderse si monetizas muchísima capacidad adicional, pero justo los pasillos finales concentran stock con necesidad AM/TR y eso invalida gran parte del supuesto.
- Solo tendría sentido abrir articulada si primero rediseñas la tipología de los propietarios de cola o aceptas que parte del beneficio de capacidad se te va a ir en spillover a pasillos reach.
"""
    output_path = detail_dir / "resumen_rentabilidad_2026.md"
    output_path.write_text(text, encoding="utf-8")
    return output_path


def run_mahou_profitability_2026(
    base_dir: Path,
    output_root: str = "mahou_codex_rotacion_2026_rentabilidad",
) -> dict[str, str]:
    detail_dir = base_dir / "output" / output_root / "detail"
    detail_dir.mkdir(parents=True, exist_ok=True)

    sources = _load_inputs(base_dir)
    picks = _prepare_pick_lines(sources["movements"])
    alloc = _layout_allocations(sources["layout"], sources["demand"], sources["owner_abc"])

    owner_table = _owner_productivity_table(picks, alloc)
    by_aisle = _aisle_mix_table(alloc)
    scenarios, sensitivity = _scenario_table(owner_table, by_aisle)
    uncovered = _uncovered_owner_table(picks, owner_table, sources["owner_abc"])
    assumptions = _assumptions_table()
    references = _references_table()
    costs = _cost_table()

    tables: dict[str, pd.DataFrame] = {
        "tabla_supuestos_rentabilidad_2026": assumptions,
        "tabla_fuentes_rentabilidad_2026": references,
        "tabla_costes_mercado_carretillas": costs,
        "tabla_productividad_propietario_2026": owner_table,
        "tabla_propietarios_picking_sin_layout_2026": uncovered,
        "tabla_pasillo_rotacion_amtr_2026": by_aisle,
        "tabla_escenarios_rentabilidad_2026": scenarios,
        "tabla_sensibilidad_pasillos_articulados": sensitivity,
    }

    outputs: dict[str, str] = {}
    for name, frame in tables.items():
        path = detail_dir / f"{name}.csv"
        frame.to_csv(path, index=False, encoding="utf-8-sig")
        outputs[name] = str(path)

    chart_outputs = _render_charts(detail_dir, scenarios, sensitivity, owner_table, by_aisle)
    outputs.update(chart_outputs)

    summary_md = _render_markdown(detail_dir, scenarios, sensitivity, owner_table, uncovered)
    outputs["resumen_rentabilidad_2026"] = str(summary_md)

    manifest_path = detail_dir / "workbook_tables_detail.json"
    manifest_path.write_text(
        json.dumps({name: _json_safe_records(frame) for name, frame in tables.items()}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    outputs["workbook_manifest"] = str(manifest_path)
    return outputs
