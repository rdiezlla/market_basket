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
from market_basket.transactions import build_transactions


class TransactionsTestCase(unittest.TestCase):
    def test_build_transactions_respects_separator_and_metrics(self) -> None:
        config = AppConfig()
        config.transaction.id_separator = "__"
        clean_df = pd.DataFrame(
            [
                {
                    "external_order": "O1",
                    "owner": "P1",
                    "article": "A",
                    "article_description": "A desc",
                    "location": "L1",
                    "quantity": 2,
                    "completion_date": pd.Timestamp("2024-01-01 10:00:00"),
                },
                {
                    "external_order": "O1",
                    "owner": "P1",
                    "article": "A",
                    "article_description": "A desc",
                    "location": "L2",
                    "quantity": 1,
                    "completion_date": pd.Timestamp("2024-01-01 10:05:00"),
                },
                {
                    "external_order": "O1",
                    "owner": "P1",
                    "article": "B",
                    "article_description": "B desc",
                    "location": "L2",
                    "quantity": 3,
                    "completion_date": pd.Timestamp("2024-01-01 10:10:00"),
                },
            ]
        )

        result = build_transactions(clean_df, config)
        tx = result.transactions_df.iloc[0]
        self.assertEqual(tx["transaction_id"], "O1__P1")
        self.assertEqual(int(tx["unique_locations_in_basket"]), 2)
        self.assertTrue(bool(tx["repeated_sku_flag"]))
        self.assertGreater(float(tx["basket_dispersion_proxy"]), 0)


if __name__ == "__main__":
    unittest.main()
