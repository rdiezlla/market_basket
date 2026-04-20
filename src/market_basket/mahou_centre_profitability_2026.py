from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .mahou_dimensioning import _normalize_dataframe_columns, _prepare_movements


PERIOD_START = pd.Timestamp("2026-01-01")
PERIOD_END = pd.Timestamp("2026-04-20 23:59:59")
ANNUALIZATION_FACTOR = 365 / 110
LABOUR_EUR_H = 22.0
RETRACT_MONTHLY_EUR = 1100.0
ARTICULATED_MONTHLY_EUR = 1500.0
TRUCK_PRODUCTIVE_H_PER_MONTH = 160.0
FTE_H_PER_YEAR = 1760.0
ACTIVE_SOURCE_AISLES = [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 20]
SEC_PER_JUMP_BASE = 60.0
SEC_PER_JUMP_SENSITIVITY = [45.0, 60.0, 75.0, 90.0]


def _json_safe_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    export = frame.copy()
    for column in export.columns:
        if pd.api.types.is_datetime64_any_dtype(export[column]):
            export[column] = export[column].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
    return json.loads(export.to_json(orient="records", date_format="iso"))


def _load_inputs(base_dir: Path) -> dict[str, pd.DataFrame]:
    movements = _normalize_dataframe_columns(pd.read_excel(base_dir / "movimientos.xlsx"))
    layout = pd.read_csv(
        base_dir / "output" / "mahou_codex_rotacion_2026" / "detail" / "tabla_destino_propietario_rangos_resumen.csv"
    )
    owner_abc = pd.read_csv(
        base_dir / "output" / "mahou_codex_rotacion_2026" / "detail" / "tabla_owner_picking_abc_2026.csv"
    )
    return {
        "movements": movements,
        "layout": layout,
        "owner_abc": owner_abc,
    }


def _prepare_pick_lines(movements: pd.DataFrame) -> pd.DataFrame:
    prepared = _prepare_movements(movements).copy()
    prepared = prepared[
        prepared["fecha_finalizacion"].between(PERIOD_START, PERIOD_END, inclusive="both")
    ].copy()
    prepared = prepared[prepared["pasillo_origen"].isin(ACTIVE_SOURCE_AISLES)].copy()
    aisle_rank = {aisle: index + 1 for index, aisle in enumerate(ACTIVE_SOURCE_AISLES)}
    prepared["source_rank"] = prepared["pasillo_origen"].map(aisle_rank)
    prepared["duracion_seg"] = (
        prepared["fecha_finalizacion"] - prepared["fecha_inicio"]
    ).dt.total_seconds()
    prepared["dia"] = prepared["fecha_finalizacion"].dt.date
    prepared = prepared.sort_values(
        ["operario", "dia", "propietario", "fecha_inicio", "fecha_finalizacion"]
    ).reset_index(drop=True)
    return prepared


def _destination_aisle_counts(layout: pd.DataFrame) -> pd.DataFrame:
    export = layout.copy()
    export["propietario"] = pd.to_numeric(export["propietario"], errors="coerce")
    grouped = (
        export.groupby(["propietario", "owner_name"], dropna=False)["pasillo_destino"]
        .nunique()
        .reset_index(name="n_pasillos_destino_rotacion_2026")
    )
    return grouped


def _jump_calibration(picks: pd.DataFrame) -> pd.DataFrame:
    calibration = picks.copy()
    for column in ["operario", "propietario", "dia"]:
        calibration[f"prev_{column}"] = calibration[column].shift(1)
    calibration["prev_source_rank"] = calibration["source_rank"].shift(1)
    calibration["prev_end"] = calibration["fecha_finalizacion"].shift(1)
    calibration["same_operator_day_owner"] = (
        (calibration["operario"] == calibration["prev_operario"])
        & (calibration["propietario"] == calibration["prev_propietario"])
        & (calibration["dia"] == calibration["prev_dia"])
    )
    calibration = calibration[
        calibration["same_operator_day_owner"]
        & calibration["duracion_seg"].between(2, 3600)
    ].copy()
    calibration["aisle_jump"] = (
        calibration["source_rank"] - calibration["prev_source_rank"]
    ).abs()
    calibration["changed_aisle"] = calibration["aisle_jump"] > 0
    same = calibration[~calibration["changed_aisle"]]["duracion_seg"]
    diff = calibration[calibration["changed_aisle"]]["duracion_seg"]
    rows = [
        {
            "metrica": "transiciones_mismo_pasillo",
            "valor": float((~calibration["changed_aisle"]).sum()),
            "unidad": "transiciones",
            "comentario": "Consecutivas mismo operario/día/propietario",
        },
        {
            "metrica": "transiciones_cambio_pasillo",
            "valor": float(calibration["changed_aisle"].sum()),
            "unidad": "transiciones",
            "comentario": "Consecutivas mismo operario/día/propietario",
        },
        {
            "metrica": "mediana_duracion_mismo_pasillo",
            "valor": float(same.median()),
            "unidad": "seg",
            "comentario": "Base sin salto de pasillo",
        },
        {
            "metrica": "mediana_duracion_cambio_pasillo",
            "valor": float(diff.median()),
            "unidad": "seg",
            "comentario": "Incluye coste de salto de pasillo",
        },
        {
            "metrica": "delta_mediana_cambio_vs_mismo",
            "valor": float(diff.median() - same.median()),
            "unidad": "seg",
            "comentario": "Sobreprecio observado de cambiar de pasillo",
        },
        {
            "metrica": "sec_por_salto_base_modelado",
            "valor": SEC_PER_JUMP_BASE,
            "unidad": "seg/rank",
            "comentario": "Supuesto base usado en el escenario",
        },
    ]
    return pd.DataFrame(rows)


def _daily_owner_batches(
    picks: pd.DataFrame,
    destination_aisles: pd.DataFrame,
    owner_abc: pd.DataFrame,
) -> pd.DataFrame:
    owner_abc = owner_abc.copy()
    owner_abc["owner"] = pd.to_numeric(owner_abc["owner"], errors="coerce")
    rows: list[dict[str, object]] = []
    for (operario, dia, propietario), frame in picks.groupby(["operario", "dia", "propietario"], dropna=True):
        ranks = frame["source_rank"].tolist()
        jump_current = sum(abs(ranks[index] - ranks[index - 1]) for index in range(1, len(ranks)))
        unique_aisles = int(frame["source_rank"].nunique())
        rows.append(
            {
                "operario": operario,
                "dia": dia,
                "propietario": int(propietario),
                "owner_name": frame["owner"].iloc[0] if "owner" in frame.columns else "",
                "lineas_batch": int(len(frame)),
                "horas_batch_actual": float(frame["duracion_seg"].sum() / 3600),
                "pasillos_actuales_batch": ",".join(str(int(value)) for value in sorted(frame["pasillo_origen"].dropna().unique())),
                "n_pasillos_actuales_batch": unique_aisles,
                "jump_current": float(jump_current),
            }
        )
    batch = pd.DataFrame(rows)
    batch = batch.merge(destination_aisles, on="propietario", how="left")
    batch = batch.merge(
        owner_abc[["owner", "owner_name", "abc_picking_2026", "picking_lineas_2026"]],
        left_on="propietario",
        right_on="owner",
        how="left",
        suffixes=("", "_abc"),
    )
    owner_name_right = "owner_name_abc" if "owner_name_abc" in batch.columns else "owner_name_y"
    owner_name_left = "owner_name" if "owner_name" in batch.columns else "owner_name_x"
    if owner_name_right in batch.columns:
        batch[owner_name_left] = batch[owner_name_right].combine_first(batch[owner_name_left])
    batch = batch.rename(columns={owner_name_left: "owner_name"})
    batch = batch.drop(
        columns=[
            column
            for column in ["owner", "owner_name_abc", "owner_name_x", "owner_name_y"]
            if column in batch.columns and column != "owner_name"
        ]
    )
    batch["n_pasillos_destino_rotacion_2026"] = batch["n_pasillos_destino_rotacion_2026"].fillna(
        batch["n_pasillos_actuales_batch"]
    )
    batch["jump_compacto_actual"] = np.maximum(batch["n_pasillos_actuales_batch"] - 1, 0)
    batch["jump_rotacion_2026"] = np.maximum(
        np.minimum(batch["n_pasillos_actuales_batch"], batch["n_pasillos_destino_rotacion_2026"]) - 1,
        0,
    )
    for sec in SEC_PER_JUMP_SENSITIVITY:
        suffix = int(sec)
        batch[f"horas_ahorradas_compacto_{suffix}s_anual"] = (
            (batch["jump_current"] - batch["jump_compacto_actual"]).clip(lower=0) * sec / 3600 * ANNUALIZATION_FACTOR
        )
        batch[f"horas_ahorradas_rotacion_2026_{suffix}s_anual"] = (
            (batch["jump_current"] - batch["jump_rotacion_2026"]).clip(lower=0) * sec / 3600 * ANNUALIZATION_FACTOR
        )
    batch["horas_ahorradas_compacto_base_anual"] = batch[
        f"horas_ahorradas_compacto_{int(SEC_PER_JUMP_BASE)}s_anual"
    ]
    batch["horas_ahorradas_rotacion_2026_base_anual"] = batch[
        f"horas_ahorradas_rotacion_2026_{int(SEC_PER_JUMP_BASE)}s_anual"
    ]
    return batch


def _centre_scenarios(batch: pd.DataFrame, picks: pd.DataFrame) -> pd.DataFrame:
    current_hours_ytd = float(picks["duracion_seg"].sum() / 3600)
    current_hours_annual = current_hours_ytd * ANNUALIZATION_FACTOR
    saved_compacto = float(batch["horas_ahorradas_compacto_base_anual"].sum())
    saved_rotacion = float(batch["horas_ahorradas_rotacion_2026_base_anual"].sum())
    retr_cost_per_h = RETRACT_MONTHLY_EUR / TRUCK_PRODUCTIVE_H_PER_MONTH
    art_cost_per_h = ARTICULATED_MONTHLY_EUR / TRUCK_PRODUCTIVE_H_PER_MONTH
    rows = []
    for scenario, saved_hours in [
        ("actual_disperso", 0.0),
        ("actual_compacto_por_propietario", saved_compacto),
        ("layout_rotacion_2026", saved_rotacion),
    ]:
        direct_hours = current_hours_annual - saved_hours
        rows.append(
            {
                "escenario": scenario,
                "horas_directas_pi_anualizadas": round(direct_hours, 2),
                "horas_ahorradas_vs_actual": round(saved_hours, 2),
                "ahorro_personal_eur_anual": round(saved_hours * LABOUR_EUR_H, 2),
                "fte_equivalentes_recuperados": round(saved_hours / FTE_H_PER_YEAR, 3),
                "truck_years_equivalentes_recuperados": round(
                    saved_hours / (TRUCK_PRODUCTIVE_H_PER_MONTH * 12), 3
                ),
                "ahorro_equipo_retractil_equivalente_eur_anual": round(saved_hours * retr_cost_per_h, 2),
                "ahorro_equipo_articulada_equivalente_eur_anual": round(saved_hours * art_cost_per_h, 2),
                "elimina_1_fte_completo": "si" if saved_hours >= FTE_H_PER_YEAR else "no",
                "elimina_1_carretilla_completa": "si"
                if saved_hours >= TRUCK_PRODUCTIVE_H_PER_MONTH * 12
                else "no",
            }
        )
    return pd.DataFrame(rows)


def _sensitivity_table(batch: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for sec in SEC_PER_JUMP_SENSITIVITY:
        suffix = int(sec)
        for scenario, column in [
            ("actual_compacto_por_propietario", f"horas_ahorradas_compacto_{suffix}s_anual"),
            ("layout_rotacion_2026", f"horas_ahorradas_rotacion_2026_{suffix}s_anual"),
        ]:
            saved_hours = float(batch[column].sum())
            rows.append(
                {
                    "escenario": scenario,
                    "sec_por_salto": sec,
                    "horas_ahorradas_anuales": round(saved_hours, 2),
                    "ahorro_personal_eur_anual": round(saved_hours * LABOUR_EUR_H, 2),
                    "ahorro_equipo_retractil_equivalente_eur_anual": round(
                        saved_hours * (RETRACT_MONTHLY_EUR / TRUCK_PRODUCTIVE_H_PER_MONTH), 2
                    ),
                    "ahorro_equipo_articulada_equivalente_eur_anual": round(
                        saved_hours * (ARTICULATED_MONTHLY_EUR / TRUCK_PRODUCTIVE_H_PER_MONTH), 2
                    ),
                }
            )
    return pd.DataFrame(rows)


def _operator_summary(batch: pd.DataFrame, picks: pd.DataFrame) -> pd.DataFrame:
    current = (
        picks.groupby("operario")
        .agg(
            lineas_2026=("transaction_id", "count"),
            horas_pi_ytd=("duracion_seg", lambda series: float(series.sum() / 3600)),
        )
        .reset_index()
    )
    impact = (
        batch.groupby("operario")
        .agg(
            horas_ahorradas_compacto_base_anual=("horas_ahorradas_compacto_base_anual", "sum"),
            horas_ahorradas_rotacion_2026_base_anual=("horas_ahorradas_rotacion_2026_base_anual", "sum"),
        )
        .reset_index()
    )
    summary = current.merge(impact, on="operario", how="left").fillna(0)
    summary["horas_pi_anualizadas"] = summary["horas_pi_ytd"] * ANNUALIZATION_FACTOR
    summary["ahorro_personal_rotacion_2026_eur_anual"] = (
        summary["horas_ahorradas_rotacion_2026_base_anual"] * LABOUR_EUR_H
    )
    return summary.sort_values("ahorro_personal_rotacion_2026_eur_anual", ascending=False).reset_index(drop=True)


def _owner_summary(batch: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        batch.groupby(["propietario", "owner_name", "abc_picking_2026"], dropna=False)
        .agg(
            batches_2026=("dia", "count"),
            lineas_2026=("lineas_batch", "sum"),
            n_pasillos_actuales_batch_med=("n_pasillos_actuales_batch", "mean"),
            jump_actual_total=("jump_current", "sum"),
            horas_ahorradas_compacto_base_anual=("horas_ahorradas_compacto_base_anual", "sum"),
            horas_ahorradas_rotacion_2026_base_anual=("horas_ahorradas_rotacion_2026_base_anual", "sum"),
            n_pasillos_destino_rotacion_2026=("n_pasillos_destino_rotacion_2026", "max"),
        )
        .reset_index()
    )
    grouped["ahorro_personal_compacto_eur_anual"] = (
        grouped["horas_ahorradas_compacto_base_anual"] * LABOUR_EUR_H
    )
    grouped["ahorro_personal_rotacion_2026_eur_anual"] = (
        grouped["horas_ahorradas_rotacion_2026_base_anual"] * LABOUR_EUR_H
    )
    return grouped.sort_values("ahorro_personal_rotacion_2026_eur_anual", ascending=False).reset_index(drop=True)


def _assumptions_table() -> pd.DataFrame:
    rows = [
        {
            "supuesto": "scope_datos",
            "valor": "Solo movimientos PI de movimientos.xlsx",
            "unidad": "texto",
            "comentario": "El ahorro de centro se calcula sobre el flujo realmente observado en el fichero.",
        },
        {
            "supuesto": "periodo",
            "valor": f"{PERIOD_START.date().isoformat()} a {PERIOD_END.date().isoformat()}",
            "unidad": "fecha",
            "comentario": "Se anualiza multiplicando por 365/110.",
        },
        {
            "supuesto": "coste_personal",
            "valor": LABOUR_EUR_H,
            "unidad": "EUR/h",
            "comentario": "Dado por negocio.",
        },
        {
            "supuesto": "carretilla_retractil_mes",
            "valor": RETRACT_MONTHLY_EUR,
            "unidad": "EUR/mes",
            "comentario": "Dado por negocio.",
        },
        {
            "supuesto": "carretilla_articulada_mes",
            "valor": ARTICULATED_MONTHLY_EUR,
            "unidad": "EUR/mes",
            "comentario": "Dado por negocio.",
        },
        {
            "supuesto": "horas_productivas_mes_carretilla",
            "valor": TRUCK_PRODUCTIVE_H_PER_MONTH,
            "unidad": "h/mes",
            "comentario": "Usadas para convertir ahorro horario en equivalente de máquina.",
        },
        {
            "supuesto": "sec_por_salto_base",
            "valor": SEC_PER_JUMP_BASE,
            "unidad": "seg/rank",
            "comentario": "Supuesto base para monetizar dispersión entre pasillos.",
        },
        {
            "supuesto": "escenario_compacto_actual",
            "valor": "salto mínimo = n_pasillos_batch - 1",
            "unidad": "regla",
            "comentario": "Mismo almacén actual pero propietarios colocados de forma contigua.",
        },
        {
            "supuesto": "escenario_rotacion_2026",
            "valor": "salto mínimo = min(n_pasillos_batch, n_pasillos_destino) - 1",
            "unidad": "regla",
            "comentario": "Compactación + límite de pasillos del layout 2026.",
        },
    ]
    return pd.DataFrame(rows)


def _render_charts(
    detail_dir: Path,
    scenarios: pd.DataFrame,
    sensitivity: pd.DataFrame,
    owners: pd.DataFrame,
    operators: pd.DataFrame,
) -> dict[str, str]:
    charts_dir = detail_dir / "graficos_centro"
    charts_dir.mkdir(parents=True, exist_ok=True)
    outputs: dict[str, str] = {}
    plt.style.use("seaborn-v0_8-whitegrid")

    # Waterfall style bars
    fig, ax = plt.subplots(figsize=(11, 6))
    labels = ["Actual", "Compacto actual", "Rotacion 2026"]
    hours = scenarios["horas_directas_pi_anualizadas"].tolist()
    ax.bar(labels, hours, color=["#4a5568", "#2b6cb0", "#2f855a"])
    ax.set_ylabel("Horas PI anualizadas")
    ax.set_title("Horas directas PI del centro tras agrupar por propietario")
    for index, value in enumerate(hours):
        ax.text(index, value + 12, f"{value:.0f} h", ha="center", fontsize=10)
    fig.tight_layout()
    path = charts_dir / "horas_centro_escenarios.png"
    fig.savefig(path, dpi=200)
    plt.close(fig)
    outputs["horas_centro_escenarios"] = str(path)

    # Sensitivity
    fig, ax = plt.subplots(figsize=(11, 6))
    for scenario, color in [
        ("actual_compacto_por_propietario", "#2b6cb0"),
        ("layout_rotacion_2026", "#2f855a"),
    ]:
        frame = sensitivity[sensitivity["escenario"] == scenario]
        ax.plot(
            frame["sec_por_salto"],
            frame["ahorro_personal_eur_anual"],
            marker="o",
            linewidth=2.4,
            color=color,
            label=scenario.replace("_", " "),
        )
    ax.set_xlabel("Segundos modelados por salto de pasillo")
    ax.set_ylabel("EUR/año ahorro personal")
    ax.set_title("Sensibilidad del ahorro a la fricción por salto de pasillo")
    ax.legend()
    fig.tight_layout()
    path = charts_dir / "sensibilidad_ahorro_personal.png"
    fig.savefig(path, dpi=200)
    plt.close(fig)
    outputs["sensibilidad_ahorro_personal"] = str(path)

    # Top owners
    top_owners = owners.head(12).sort_values("ahorro_personal_rotacion_2026_eur_anual", ascending=True)
    fig, ax = plt.subplots(figsize=(10, 7))
    ax.barh(
        [f"{int(row.propietario)} {row.owner_name}" for row in top_owners.itertuples(index=False)],
        top_owners["ahorro_personal_rotacion_2026_eur_anual"],
        color="#2f855a",
    )
    ax.set_xlabel("EUR/año")
    ax.set_title("Top propietarios por ahorro de centro en rotacion_2026")
    fig.tight_layout()
    path = charts_dir / "top_propietarios_ahorro_centro.png"
    fig.savefig(path, dpi=200)
    plt.close(fig)
    outputs["top_propietarios_ahorro_centro"] = str(path)

    # Top operators
    top_ops = operators.head(10).sort_values("ahorro_personal_rotacion_2026_eur_anual", ascending=True)
    fig, ax = plt.subplots(figsize=(10, 7))
    ax.barh(
        [str(int(value)) for value in top_ops["operario"]],
        top_ops["ahorro_personal_rotacion_2026_eur_anual"],
        color="#3182ce",
    )
    ax.set_xlabel("EUR/año")
    ax.set_title("Operarios con más horas recuperables en 2026")
    fig.tight_layout()
    path = charts_dir / "top_operarios_ahorro_centro.png"
    fig.savefig(path, dpi=200)
    plt.close(fig)
    outputs["top_operarios_ahorro_centro"] = str(path)
    return outputs


def _render_markdown(
    detail_dir: Path,
    scenarios: pd.DataFrame,
    sensitivity: pd.DataFrame,
    owners: pd.DataFrame,
    operators: pd.DataFrame,
    assumptions: pd.DataFrame,
    calibration: pd.DataFrame,
    picks: pd.DataFrame,
) -> Path:
    current = scenarios.iloc[0]
    compact = scenarios[scenarios["escenario"] == "actual_compacto_por_propietario"].iloc[0]
    rotation = scenarios[scenarios["escenario"] == "layout_rotacion_2026"].iloc[0]
    top_owner = owners.iloc[0]
    top_operator = operators.iloc[0]
    delta_mediana = calibration.loc[
        calibration["metrica"] == "delta_mediana_cambio_vs_mismo", "valor"
    ].iloc[0]
    text = f"""# Rentabilidad de centro 2026

## Qué se ha medido

- Base real: movimientos `PI` de `{PERIOD_START.date().isoformat()}` a `{PERIOD_END.date().isoformat()}`.
- El análisis no mide ahorro “de un propietario”, sino ahorro total del centro por quitar dispersión entre pasillos dentro del mismo propietario.
- Se han comparado tres estados:
  - actual disperso
  - actual compacto por propietario dentro del almacén actual
  - layout `rotacion_2026`

## Cómo se ha monetizado

- Se han detectado batches reales `operario + día + propietario`.
- Para cada batch se ha calculado el salto actual entre pasillos y el salto mínimo si el propietario estuviera compacto.
- La calibración observada da una mediana de {delta_mediana:.1f} segundos extra al cambiar de pasillo respecto a quedarse en el mismo.
- El escenario base monetiza cada rank de salto con `{SEC_PER_JUMP_BASE:.0f}` segundos, y además se deja sensibilidad a `{', '.join(str(int(value)) for value in SEC_PER_JUMP_SENSITIVITY)}` segundos.

## Resultado para gerencia

- Horas PI anualizadas actuales observadas: {current['horas_directas_pi_anualizadas']:.2f} h/año.
- Si compactas el almacén actual por propietario: recuperas {compact['horas_ahorradas_vs_actual']:.2f} h/año, equivalentes a {compact['ahorro_personal_eur_anual']:.2f} EUR/año de personal.
- Si vas al layout `rotacion_2026`: recuperas {rotation['horas_ahorradas_vs_actual']:.2f} h/año, equivalentes a {rotation['ahorro_personal_eur_anual']:.2f} EUR/año de personal.

## Lectura de personal y carretillas

- En equivalente de plantilla, `rotacion_2026` libera {rotation['fte_equivalentes_recuperados']:.3f} FTE.
- En equivalente de máquina, `rotacion_2026` libera {rotation['truck_years_equivalentes_recuperados']:.3f} carretillas-año.
- A coste de reach, eso equivale a {rotation['ahorro_equipo_retractil_equivalente_eur_anual']:.2f} EUR/año.
- A coste de articulada, eso equivale a {rotation['ahorro_equipo_articulada_equivalente_eur_anual']:.2f} EUR/año.
- Con estos datos de `PI` no se justifica por sí solo eliminar una persona completa ni una carretilla completa; el ahorro es sobre todo de productividad recuperable y capacidad operativa redeplegable.

## Dónde está el ahorro

- El propietario que más aporta al ahorro total es {int(top_owner['propietario'])} {top_owner['owner_name']}, con {top_owner['ahorro_personal_rotacion_2026_eur_anual']:.2f} EUR/año.
- El operario con más tiempo potencialmente liberado en este flujo es {int(top_operator['operario'])}, con {top_operator['ahorro_personal_rotacion_2026_eur_anual']:.2f} EUR/año equivalentes.
- El foco del ahorro viene de pocos propietarios muy rotadores y muy dispersos: `4`, `23`, `3`, `95`, `30`, `29`.

## Conclusión

- Sí hay ahorro claro al agrupar por propietario.
- La cifra base defendible con el fichero de movimientos es del orden de {compact['ahorro_personal_eur_anual']:.0f} a {rotation['ahorro_personal_eur_anual']:.0f} EUR/año de personal directo, más {rotation['ahorro_equipo_retractil_equivalente_eur_anual']:.0f} EUR/año de capacidad-equivalente de reach si esa productividad te permite reducir uso efectivo de máquina.
- La mejora es real, pero el propio fichero `PI` acota el techo: en todo el periodo solo se observan {picks['duracion_seg'].sum()/3600:.2f} h directas de este flujo, así que no sería honesto prometer un ahorro estructural enorme de plantilla sin más datos de otras tareas.
"""
    path = detail_dir / "resumen_rentabilidad_centro_2026.md"
    path.write_text(text, encoding="utf-8")
    return path


def run_mahou_centre_profitability_2026(
    base_dir: Path,
    output_root: str = "mahou_codex_rotacion_2026_rentabilidad_centro",
) -> dict[str, str]:
    detail_dir = base_dir / "output" / output_root / "detail"
    detail_dir.mkdir(parents=True, exist_ok=True)

    sources = _load_inputs(base_dir)
    picks = _prepare_pick_lines(sources["movements"])
    destination_aisles = _destination_aisle_counts(sources["layout"])
    calibration = _jump_calibration(picks)
    batch = _daily_owner_batches(picks, destination_aisles, sources["owner_abc"])
    scenarios = _centre_scenarios(batch, picks)
    sensitivity = _sensitivity_table(batch)
    operators = _operator_summary(batch, picks)
    owners = _owner_summary(batch)
    assumptions = _assumptions_table()

    tables: dict[str, pd.DataFrame] = {
        "tabla_supuestos_rentabilidad_centro_2026": assumptions,
        "tabla_calibracion_saltos_pasillo_2026": calibration,
        "tabla_batches_owner_operario_dia_2026": batch,
        "tabla_escenarios_rentabilidad_centro_2026": scenarios,
        "tabla_sensibilidad_salto_pasillo_2026": sensitivity,
        "tabla_impacto_propietario_centro_2026": owners,
        "tabla_impacto_operario_centro_2026": operators,
    }

    outputs: dict[str, str] = {}
    for name, frame in tables.items():
        path = detail_dir / f"{name}.csv"
        frame.to_csv(path, index=False, encoding="utf-8-sig")
        outputs[name] = str(path)

    chart_outputs = _render_charts(detail_dir, scenarios, sensitivity, owners, operators)
    outputs.update(chart_outputs)

    summary_md = _render_markdown(
        detail_dir,
        scenarios,
        sensitivity,
        owners,
        operators,
        assumptions,
        calibration,
        picks,
    )
    outputs["resumen_rentabilidad_centro_2026"] = str(summary_md)

    manifest_path = detail_dir / "workbook_tables_detail.json"
    manifest_path.write_text(
        json.dumps({name: _json_safe_records(frame) for name, frame in tables.items()}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    outputs["workbook_manifest"] = str(manifest_path)
    return outputs
