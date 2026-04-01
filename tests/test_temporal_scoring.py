from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from market_basket.config import AppConfig
from market_basket.associations import compute_associations
from market_basket.scoring import compute_layout_scores
from market_basket.temporal import compute_temporal_stability


class TemporalScoringTestCase(unittest.TestCase):
    def test_temporal_stability_and_scoring_outputs(self) -> None:
        config = AppConfig()
        tx_item_df = pd.DataFrame(
            [
                {"transaction_id": "T1", "article": "A", "quantity_sum": 1.0, "last_completion_date": pd.Timestamp("2024-01-10")},
                {"transaction_id": "T1", "article": "B", "quantity_sum": 1.0, "last_completion_date": pd.Timestamp("2024-01-10")},
                {"transaction_id": "T2", "article": "A", "quantity_sum": 1.0, "last_completion_date": pd.Timestamp("2024-04-10")},
                {"transaction_id": "T2", "article": "B", "quantity_sum": 2.0, "last_completion_date": pd.Timestamp("2024-04-10")},
                {"transaction_id": "T3", "article": "A", "quantity_sum": 1.0, "last_completion_date": pd.Timestamp("2024-07-10")},
                {"transaction_id": "T3", "article": "B", "quantity_sum": 2.0, "last_completion_date": pd.Timestamp("2024-07-10")},
                {"transaction_id": "T4", "article": "A", "quantity_sum": 1.0, "last_completion_date": pd.Timestamp("2024-10-10")},
                {"transaction_id": "T4", "article": "C", "quantity_sum": 1.0, "last_completion_date": pd.Timestamp("2024-10-10")},
            ]
        )

        associations = compute_associations(tx_item_df, config, min_pair_transactions_override=1)
        temporal = compute_temporal_stability(tx_item_df, config)
        scoring = compute_layout_scores(associations.pair_metrics, temporal.stability_metrics, config)

        self.assertIn("support_slope", temporal.stability_metrics.columns)
        self.assertIn("trend_classification", temporal.stability_metrics.columns)
        self.assertIn("layout_action_hint", scoring.scored_pairs.columns)
        self.assertIn("candidate_same_zone", scoring.scored_pairs.columns)
        self.assertTrue(scoring.scored_pairs["final_layout_score"].between(0, 1).all())


if __name__ == "__main__":
    unittest.main()
