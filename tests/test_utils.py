from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from market_basket.utils import canonicalize_identifier, clean_string


class UtilsTestCase(unittest.TestCase):
    def test_canonicalize_identifier(self) -> None:
        self.assertEqual(canonicalize_identifier("34.0"), "34")
        self.assertEqual(canonicalize_identifier("TPA1737"), "TPA1737")
        self.assertIsNone(canonicalize_identifier(""))
        self.assertIsNone(canonicalize_identifier(None))

    def test_clean_string(self) -> None:
        self.assertEqual(clean_string("  MESA   ROJA  "), "MESA ROJA")
        self.assertIsNone(clean_string(""))
        self.assertIsNone(clean_string(None))


if __name__ == "__main__":
    unittest.main()
