from __future__ import annotations

import unittest

from market_basket.mahou_dimensioning import (
    _tipology_to_eu_eq,
    _tipology_to_modules,
    classify_tipology,
)


class MahouDimensioningTests(unittest.TestCase):
    def test_classify_balda_slot(self) -> None:
        self.assertEqual(classify_tipology(10, 61, 1, "BD"), "balda_9h")
        self.assertEqual(classify_tipology(11, 44, 9, "BD"), "balda_9h")

    def test_classify_special_floor_ranges(self) -> None:
        self.assertEqual(classify_tipology(7, 15, 0, "S"), "suelo_250")
        self.assertEqual(classify_tipology(12, 2, 0, "S"), "suelo_300")
        self.assertEqual(classify_tipology(14, 24, 0, "S"), "suelo_126")

    def test_classify_standard_positions(self) -> None:
        self.assertEqual(classify_tipology(6, 128, 20, "AM"), "AM")
        self.assertEqual(classify_tipology(6, 80, 0, "S"), "suelo_estandar")

    def test_balfa_modules_do_not_mix_units(self) -> None:
        self.assertAlmostEqual(_tipology_to_modules(540, "balda_9h"), 20.0, places=4)
        self.assertAlmostEqual(_tipology_to_eu_eq("balda_9h"), 1 / 9, places=6)


if __name__ == "__main__":
    unittest.main()
