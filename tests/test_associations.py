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


class AssociationsTestCase(unittest.TestCase):
    def test_compute_associations_adds_npmi_and_residuals(self) -> None:
        config = AppConfig()
        config.thresholds.rules.min_lift = 0.0
        tx_item_df = pd.DataFrame(
            [
                {"transaction_id": "T1", "article": "A", "quantity_sum": 2.0},
                {"transaction_id": "T1", "article": "B", "quantity_sum": 1.0},
                {"transaction_id": "T2", "article": "A", "quantity_sum": 2.0},
                {"transaction_id": "T2", "article": "B", "quantity_sum": 2.0},
                {"transaction_id": "T3", "article": "A", "quantity_sum": 1.0},
                {"transaction_id": "T3", "article": "C", "quantity_sum": 1.0},
                {"transaction_id": "T4", "article": "B", "quantity_sum": 1.0},
                {"transaction_id": "T4", "article": "C", "quantity_sum": 1.0},
                {"transaction_id": "T5", "article": "A", "quantity_sum": 1.0},
                {"transaction_id": "T5", "article": "B", "quantity_sum": 1.0},
            ]
        )

        result = compute_associations(tx_item_df, config, min_pair_transactions_override=1)

        self.assertIn("npmi", result.pair_metrics.columns)
        self.assertIn("residual_cooccurrence", result.pair_metrics.columns)
        self.assertIn("expected_joint_support", result.pair_metrics.columns)
        self.assertFalse(result.rule_metrics.empty)


if __name__ == "__main__":
    unittest.main()
