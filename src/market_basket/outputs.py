from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd

from .config import AppConfig
from .similarity import build_top_item_similarity_matrix
from .utils import ensure_directory, write_json


@dataclass
class OutputPaths:
    base_dir: Path
    plots_dir: Path
    logs_dir: Path


def prepare_output_paths(config: AppConfig) -> OutputPaths:
    return OutputPaths(
        base_dir=ensure_directory(Path(config.paths.output_dir)),
        plots_dir=ensure_directory(Path(config.paths.plots_dir)),
        logs_dir=ensure_directory(Path(config.paths.logs_dir)),
    )


def save_dataframe(name: str, dataframe: pd.DataFrame, paths: OutputPaths, config: AppConfig) -> None:
    export_df = dataframe.copy()
    if config.outputs.write_csv:
        export_df.to_csv(paths.base_dir / f"{name}.csv", index=False)
    if config.outputs.write_parquet:
        export_df.to_parquet(paths.base_dir / f"{name}.parquet", index=False)
    if config.outputs.write_excel:
        export_df.to_excel(paths.base_dir / f"{name}.xlsx", index=False)


def save_quality_logs(excluded_missing_order: pd.DataFrame, null_summary: pd.DataFrame, paths: OutputPaths) -> None:
    if not excluded_missing_order.empty:
        excluded_missing_order.to_csv(paths.logs_dir / "filas_excluidas_sin_pedido_externo.csv", index=False)
    null_summary.to_csv(paths.logs_dir / "resumen_nulos.csv", index=False)


def save_metadata(metadata: dict, paths: OutputPaths) -> None:
    write_json(paths.base_dir / "metadata_modelo.json", metadata)


def _save_figure(fig: plt.Figure, path: Path) -> None:
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _safe_plot_condition(frame: pd.DataFrame | None, min_rows: int = 1) -> bool:
    return frame is not None and not frame.empty and len(frame) >= min_rows


def create_visualizations(
    transactions_df: pd.DataFrame,
    article_summary: pd.DataFrame,
    scored_pairs: pd.DataFrame,
    raw_temporal_pairs: pd.DataFrame,
    graph_edges: pd.DataFrame,
    paths: OutputPaths,
    config: AppConfig,
) -> list[str]:
    generated: list[str] = []

    if _safe_plot_condition(transactions_df):
        fig, ax = plt.subplots(figsize=(10, 5))
        clip_upper = max(5, min(20, int(transactions_df["basket_size"].max())))
        transactions_df["basket_size"].clip(upper=clip_upper).plot.hist(bins=min(20, clip_upper), ax=ax, color="#1f77b4")
        ax.set_title("Distribucion del tamano de cesta")
        ax.set_xlabel("Articulos unicos por transaccion")
        ax.set_ylabel("Transacciones")
        path = paths.plots_dir / "hist_tamano_cesta.png"
        _save_figure(fig, path)
        generated.append(path.name)

    if _safe_plot_condition(article_summary):
        top_articles = article_summary.head(config.model.top_n_articles)
        fig, ax = plt.subplots(figsize=(12, 6))
        positions = range(len(top_articles))
        labels = top_articles["article"].astype(str).tolist()
        ax.bar(list(positions), top_articles["transaction_frequency"], color="#2a9d8f")
        ax.set_title("Top articulos por frecuencia transaccional")
        ax.set_xlabel("Articulo")
        ax.set_ylabel("Transacciones")
        ax.set_xticks(list(positions))
        ax.set_xticklabels(labels, rotation=90)
        path = paths.plots_dir / "top_articulos_frecuencia.png"
        _save_figure(fig, path)
        generated.append(path.name)

        metric = config.model.heatmap_metric if config.model.heatmap_metric in scored_pairs.columns else "final_layout_score"
        top_items = top_articles["article"].astype(str).head(min(config.performance.heatmap_top_n, len(top_articles))).tolist()
        matrix_source = scored_pairs[["article_a", "article_b", metric]].rename(columns={metric: "final_layout_score"})
        similarity_matrix = build_top_item_similarity_matrix(matrix_source, top_items)
        if not similarity_matrix.empty:
            fig, ax = plt.subplots(figsize=(10, 8))
            image = ax.imshow(similarity_matrix.values, cmap="YlGnBu", aspect="auto")
            ax.set_title(f"Heatmap de afinidad top SKUs ({metric})")
            ax.set_xticks(range(len(top_items)))
            ax.set_xticklabels(top_items, rotation=90)
            ax.set_yticks(range(len(top_items)))
            ax.set_yticklabels(top_items)
            fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04)
            path = paths.plots_dir / "heatmap_coocurrencia_top.png"
            _save_figure(fig, path)
            generated.append(path.name)

    if _safe_plot_condition(scored_pairs):
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.hist(scored_pairs["final_layout_score"], bins=min(30, max(5, len(scored_pairs))), color="#e76f51")
        ax.set_title("Distribucion del score de layout")
        ax.set_xlabel("Score")
        ax.set_ylabel("Pares")
        path = paths.plots_dir / "distribucion_scores_layout.png"
        _save_figure(fig, path)
        generated.append(path.name)

    if _safe_plot_condition(graph_edges):
        graph = nx.Graph()
        for row in graph_edges.head(config.performance.graph_plot_max_edges).itertuples(index=False):
            graph.add_edge(row.article_a, row.article_b, weight=row.final_layout_score)
        if graph.number_of_edges() > 0:
            pos = nx.spring_layout(graph, seed=42, weight="weight")
            fig, ax = plt.subplots(figsize=(12, 10))
            nx.draw_networkx(
                graph,
                pos=pos,
                ax=ax,
                node_size=260,
                font_size=7,
                width=[max(0.5, data["weight"] * 3) for _, _, data in graph.edges(data=True)],
                edge_color="#6c757d",
                node_color="#264653",
                font_color="white",
            )
            ax.set_title("Red de afinidad entre SKUs")
            ax.axis("off")
            path = paths.plots_dir / "grafo_afinidad_skus.png"
            _save_figure(fig, path)
            generated.append(path.name)

    if _safe_plot_condition(raw_temporal_pairs, min_rows=2):
        top_pairs = scored_pairs[["article_a", "article_b"]].head(config.model.key_relationships_to_plot)
        subset = raw_temporal_pairs.merge(top_pairs, on=["article_a", "article_b"], how="inner")
        subset = subset[subset["period_granularity"] == "quarter"].copy()
        if not subset.empty:
            fig, ax = plt.subplots(figsize=(12, 6))
            for (article_a, article_b), frame in subset.groupby(["article_a", "article_b"]):
                ordered = frame.sort_values("period_sort_key")
                ax.plot(ordered["period_start"], ordered["joint_support"], marker="o", label=f"{article_a}-{article_b}")
            ax.set_title("Evolucion temporal de relaciones clave")
            ax.set_xlabel("Periodo")
            ax.set_ylabel("Soporte conjunto")
            ax.tick_params(axis="x", rotation=45)
            ax.legend(fontsize=7)
            path = paths.plots_dir / "evolucion_relaciones_clave.png"
            _save_figure(fig, path)
            generated.append(path.name)

    return generated


def render_executive_summary(
    summary_path: Path,
    quality_summary: pd.DataFrame,
    kpi_summary: pd.DataFrame,
    scored_pairs: pd.DataFrame,
    clusters_df: pd.DataFrame,
    hubs_df: pd.DataFrame,
    stability_metrics: pd.DataFrame,
) -> None:
    top_pairs = scored_pairs.sort_values(["final_layout_score", "shared_transactions"], ascending=[False, False]).head(10)
    strong_but_rare = scored_pairs.sort_values(["lift", "shared_transactions"], ascending=[False, True]).head(10)
    trend_columns = ["article_a", "article_b", "trend_classification", "support_trend", "lift_trend"]
    if all(column in scored_pairs.columns for column in trend_columns[2:]):
        emerging_source = scored_pairs.copy()
    else:
        available_columns = [column for column in trend_columns if column in stability_metrics.columns]
        emerging_source = scored_pairs.merge(stability_metrics[available_columns], on=["article_a", "article_b"], how="left")
    if all(column in emerging_source.columns for column in ["trend_classification", "support_trend", "lift_trend"]):
        growing_mask = (
            emerging_source["trend_classification"].eq("growing")
            | emerging_source["support_trend"].eq("growing")
            | emerging_source["lift_trend"].eq("growing")
        )
        emerging_pairs = emerging_source[growing_mask].sort_values(["final_layout_score", "shared_transactions"], ascending=[False, False]).head(10)
    else:
        emerging_pairs = emerging_source.head(0)

    lines = ["# Resumen ejecutivo automatico", ""]
    if not kpi_summary.empty:
        row = kpi_summary.iloc[0]
        lines.extend(
            [
                "## KPIs principales",
                f"- Movimientos PI validos para modelo: {int(row['movimientos_pi'])}",
                f"- Transacciones validas: {int(row['transacciones_validas'])}",
                f"- SKUs unicos: {int(row['articulos_unicos'])}",
                f"- Propietarios unicos: {int(row['propietarios_unicos'])}",
                f"- Cesta media: {row['tamano_cesta_media']:.2f}",
                "",
            ]
        )

    lines.append("## Oportunidades prioritarias de cercania")
    for row in top_pairs.itertuples(index=False):
        lines.append(
            f"- {row.article_a} + {row.article_b}: score {row.final_layout_score:.3f}, "
            f"frecuencia {row.shared_transactions}, recomendacion {row.proximity_recommendation}, "
            f"accion sugerida: {row.layout_action_hint}"
        )
    lines.append("")

    lines.append("## Relaciones fuertes pero con baja recurrencia")
    for row in strong_but_rare.itertuples(index=False):
        lines.append(
            f"- {row.article_a} + {row.article_b}: lift {row.lift:.2f}, frecuencia {row.shared_transactions}, "
            f"revision manual {bool(row.candidate_manual_review)}"
        )
    lines.append("")

    lines.append("## Relaciones emergentes")
    for row in emerging_pairs.itertuples(index=False):
        lines.append(
            f"- {row.article_a} + {row.article_b}: score {row.final_layout_score:.3f}, frecuencia {row.shared_transactions}, "
            f"tendencia soporte {getattr(row, 'support_trend', 'n/a')}, tendencia lift {getattr(row, 'lift_trend', 'n/a')}"
        )
    lines.append("")

    if not clusters_df.empty:
        lines.append("## Clusters operativos destacados")
        for row in clusters_df.head(5).itertuples(index=False):
            articles = ", ".join(list(row.articles)[:8])
            lines.append(
                f"- Cluster {row.cluster_id}: tamano {row.cluster_size}, densidad {row.density:.2f}, "
                f"score medio {row.mean_intra_cluster_score:.3f}, lift medio {row.mean_intra_cluster_lift:.2f}, articulos: {articles}"
            )
        lines.append("")

    if not hubs_df.empty:
        lines.append("## SKUs hub operativos")
        for row in hubs_df.head(5).itertuples(index=False):
            lines.append(
                f"- {row.article}: grado {row.hub_degree}, peso acumulado {row.hub_weighted_degree:.2f}, betweenness {row.hub_betweenness:.3f}"
            )
        lines.append("")

    lines.append("## Riesgos y sesgos de datos")
    for row in quality_summary.sort_values(["count", "issue"], ascending=[False, True]).head(8).itertuples(index=False):
        lines.append(f"- {row.issue}: {row.count} filas. {row.description}")

    summary_path.write_text("\n".join(lines), encoding="utf-8")
