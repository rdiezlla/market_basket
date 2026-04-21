from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from market_basket.mahou_beneficio_layout_actual_2026_v2 import run_beneficio_layout_actual_2026_v2


def main() -> None:
    outputs = run_beneficio_layout_actual_2026_v2(ROOT)
    for key, value in outputs.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
