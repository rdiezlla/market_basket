from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from market_basket.config import AppConfig
from market_basket.cleaning import clean_movements


def build_config() -> AppConfig:
    return AppConfig()


class CleaningTestCase(unittest.TestCase):
    def test_clean_movements_excludes_non_positive_quantity_and_builds_location_profile(self) -> None:
        config = build_config()
        raw = pd.DataFrame(
            [
                {
                    "movement_type": "PI",
                    "completion_date": "2024-01-01 10:00:00",
                    "article": "A",
                    "article_description": "Article A",
                    "quantity": 2,
                    "owner": "1",
                    "location": "L1",
                    "external_order": "O1",
                },
                {
                    "movement_type": "PI",
                    "completion_date": "2024-01-01 10:05:00",
                    "article": "A",
                    "article_description": "Article A",
                    "quantity": 0,
                    "owner": "1",
                    "location": "L2",
                    "external_order": "O1",
                },
                {
                    "movement_type": "PI",
                    "completion_date": "2024-01-01 10:10:00",
                    "article": "B",
                    "article_description": "Article B",
                    "quantity": 1,
                    "owner": "1",
                    "location": "L1",
                    "external_order": "O1",
                },
            ]
        )

        result = clean_movements(raw, config)

        self.assertEqual(len(result.clean_df), 2)
        self.assertIn("rows_excluded_non_positive_quantity_in_pi", result.quality_summary["issue"].tolist())
        self.assertIn("dominant_location_share", result.sku_attributes.columns)
        self.assertIn("latest_location", result.sku_attributes.columns)
        self.assertIn("multi_location_flag", result.sku_attributes.columns)
        self.assertIn("is_primary_location", result.sku_location_profile.columns)

    def test_clean_movements_removes_duplicates_when_configured(self) -> None:
        config = build_config()
        config.data_quality.drop_exact_duplicates = True
        raw = pd.DataFrame(
            [
                {
                    "movement_type": "PI",
                    "completion_date": "2024-01-01 10:00:00",
                    "article": "A",
                    "article_description": "Article A",
                    "quantity": 2,
                    "owner": "1",
                    "location": "L1",
                    "external_order": "O1",
                },
                {
                    "movement_type": "PI",
                    "completion_date": "2024-01-01 10:00:00",
                    "article": "A",
                    "article_description": "Article A",
                    "quantity": 2,
                    "owner": "1",
                    "location": "L1",
                    "external_order": "O1",
                },
            ]
        )

        result = clean_movements(raw, config)
        self.assertEqual(len(result.clean_df), 1)
        removed = result.quality_summary.loc[result.quality_summary["issue"] == "exact_duplicates_removed", "count"].iloc[0]
        self.assertEqual(int(removed), 1)


if __name__ == "__main__":
    unittest.main()
