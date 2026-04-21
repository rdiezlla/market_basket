from __future__ import annotations

import unittest

from market_basket.mahou_beneficio_layout_actual_2026_v2 import (
    _component_seconds,
    _count_aisle_changes,
    _count_contiguous_blocks,
    _count_route_reengagements,
)


class MahouBenefitLayoutActual2026V2Tests(unittest.TestCase):
    def test_count_contiguous_blocks(self) -> None:
        self.assertEqual(_count_contiguous_blocks([6, 7, 8]), 1)
        self.assertEqual(_count_contiguous_blocks([6, 7, 9, 10, 12]), 3)

    def test_count_aisle_changes(self) -> None:
        self.assertEqual(_count_aisle_changes([6, 6, 7, 7, 9]), 2)
        self.assertEqual(_count_aisle_changes([]), 0)

    def test_count_route_reengagements(self) -> None:
        self.assertEqual(_count_route_reengagements([6, 7, 6]), 1)
        self.assertEqual(_count_route_reengagements([6, 7, 8]), 0)

    def test_component_seconds_is_not_distance_only(self) -> None:
        components = _component_seconds(
            scenario="B_base_recomendado",
            meters=10.0,
            aisle_changes=2,
            stops=3,
            owner_fragment_proxy=1.0,
            discontinuous_block_proxy=1.0,
            search_events=2.0,
            maneuver_events=4.0,
            route_reengagements=1,
        )
        self.assertGreater(components["model_seconds_total"], components["seconds_meters"])
        self.assertGreater(components["seconds_aisle_change"], 0.0)
        self.assertGreater(components["seconds_owner_fragment"], 0.0)


if __name__ == "__main__":
    unittest.main()
