from __future__ import annotations

import pandas as pd


def build_top_item_similarity_matrix(pair_df: pd.DataFrame, top_articles: list[str]) -> pd.DataFrame:
    matrix = pd.DataFrame(0.0, index=top_articles, columns=top_articles)
    if pair_df.empty or not top_articles:
        return matrix
    filtered = pair_df[
        pair_df["article_a"].isin(top_articles) & pair_df["article_b"].isin(top_articles)
    ][["article_a", "article_b", "final_layout_score"]]
    for row in filtered.itertuples(index=False):
        matrix.loc[row.article_a, row.article_b] = row.final_layout_score
        matrix.loc[row.article_b, row.article_a] = row.final_layout_score
    for article in top_articles:
        matrix.loc[article, article] = 1.0
    return matrix
