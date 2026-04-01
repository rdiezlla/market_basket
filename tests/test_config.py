from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from market_basket.config import AppConfig, validate_config


class ConfigValidationTestCase(unittest.TestCase):
    def test_validate_config_normalizes_score_weights(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "input.csv"
            input_path.write_text("a,b\n1,2\n", encoding="utf-8")

            config = AppConfig()
            config.paths.input_data = str(input_path)
            config.model.score_weights = {
                "joint_frequency": 3.0,
                "lift": 1.0,
                "balanced_confidence": 1.0,
                "similarity": 1.0,
                "temporal_stability": 2.0,
                "weighted_volume": 2.0,
            }

            validated = validate_config(config)
            self.assertAlmostEqual(sum(validated.model.score_weights.values()), 1.0)

    def test_validate_config_rejects_invalid_bins(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "input.csv"
            input_path.write_text("a,b\n1,2\n", encoding="utf-8")

            config = AppConfig()
            config.paths.input_data = str(input_path)
            config.thresholds.scoring.proximity_bins = [0.5, 0.4]

            with self.assertRaises(ValueError):
                validate_config(config)


if __name__ == "__main__":
    unittest.main()
