from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from market_basket.config import load_config
from market_basket.pipeline import run_pipeline
from market_basket.utils import setup_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline de market basket para optimización de layout de picking.")
    parser.add_argument("--config", default=str(ROOT / "config" / "default_config.yaml"), help="Ruta al YAML de configuración.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    setup_logging(Path(config.paths.logs_dir) / "pipeline.log", level=config.log_level)
    run_pipeline(config)


if __name__ == "__main__":
    main()
