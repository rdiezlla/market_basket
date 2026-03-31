from __future__ import annotations

from dataclasses import dataclass

import networkx as nx
import pandas as pd

from .config import AppConfig


@dataclass
class ClusterResult:
    cluster_summary: pd.DataFrame
    hub_summary: pd.DataFrame
    graph_edges: pd.DataFrame


def build_clusters(
    scored_pairs: pd.DataFrame,
    article_summary: pd.DataFrame,
    config: AppConfig,
) -> ClusterResult:
    if scored_pairs.empty:
        return ClusterResult(pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

    min_shared = int(max(2, config.model.min_pair_transactions or 3))
    graph_edges = scored_pairs[
        (scored_pairs["final_layout_score"] >= config.model.cluster_similarity_threshold)
        & (scored_pairs["shared_transactions"] >= min_shared)
    ].copy()
    graph_edges = graph_edges.head(config.model.max_edges_for_clustering).reset_index(drop=True)

    graph = nx.Graph()
    for row in graph_edges.itertuples(index=False):
        graph.add_edge(
            row.article_a,
            row.article_b,
            weight=float(row.final_layout_score),
            shared_transactions=int(row.shared_transactions),
        )

    if graph.number_of_nodes() == 0:
        return ClusterResult(pd.DataFrame(), pd.DataFrame(), graph_edges)

    betweenness = nx.betweenness_centrality(graph, weight="weight")
    communities = list(nx.algorithms.community.greedy_modularity_communities(graph, weight="weight"))
    article_frequency_map = article_summary.set_index("article")["transaction_frequency"].to_dict()

    cluster_rows: list[dict] = []
    for cluster_id, community in enumerate(communities, start=1):
        articles = sorted(community)
        if len(articles) < config.model.cluster_min_size:
            continue
        subgraph = graph.subgraph(articles).copy()
        edge_weights = [data["weight"] for _, _, data in subgraph.edges(data=True)]
        cluster_rows.append(
            {
                "cluster_id": cluster_id,
                "articles": articles,
                "cluster_size": len(articles),
                "cluster_frequency_aggregate": int(sum(article_frequency_map.get(article, 0) for article in articles)),
                "cluster_cohesion_score": float(sum(edge_weights) / max(len(edge_weights), 1)),
                "edge_count": int(subgraph.number_of_edges()),
            }
        )

    cluster_summary = pd.DataFrame(cluster_rows)
    if not cluster_summary.empty:
        cluster_summary = cluster_summary.sort_values(
            ["cluster_cohesion_score", "cluster_size", "cluster_frequency_aggregate"],
            ascending=[False, False, False],
        ).reset_index(drop=True)

    hub_rows = []
    for node in graph.nodes:
        hub_rows.append(
            {
                "article": node,
                "hub_degree": int(graph.degree(node)),
                "hub_weighted_degree": float(graph.degree(node, weight="weight")),
                "hub_betweenness": float(betweenness.get(node, 0.0)),
            }
        )
    hub_summary = pd.DataFrame(hub_rows)
    if not hub_summary.empty:
        hub_summary = hub_summary.sort_values(["hub_weighted_degree", "hub_degree"], ascending=[False, False]).reset_index(drop=True)

    return ClusterResult(cluster_summary=cluster_summary, hub_summary=hub_summary, graph_edges=graph_edges)
