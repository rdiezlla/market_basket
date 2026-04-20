from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from market_basket.mahou_layout_detail import run_mahou_layout_detail


def main() -> None:
    outputs = run_mahou_layout_detail(
        ROOT,
        detail_output_root="mahou_codex_salidas_strict",
        ranking_mode="salidas_strict",
    )
    for key, value in outputs.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
