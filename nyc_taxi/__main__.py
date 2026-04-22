"""
CLI entry point for the ETL pipeline.

Run from the project root so `nyc_taxi` resolves as a package::

    python -m nyc_taxi
    python -m nyc_taxi --no-charts -q
"""
from __future__ import annotations

import argparse
import sys

from nyc_taxi.config import default_config
from nyc_taxi.pipeline import run_pipeline


def main() -> int:
    """Parse arguments and run :func:`nyc_taxi.pipeline.run_pipeline`."""
    p = argparse.ArgumentParser(
        description="NYC TLC Yellow Taxi ETL: download, clean, Gold Parquet, KPI CSV/PNG."
    )
    p.add_argument(
        "--no-charts",
        action="store_true",
        help="Write CSV KPIs only; skip matplotlib PNG exports (faster).",
    )
    p.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Less console output.",
    )
    args = p.parse_args()
    result = run_pipeline(
        default_config,
        verbose=not args.quiet,
        skip_charts=args.no_charts,
    )
    if not args.quiet:
        print(
            f"\nDone. Gold: {result.gold_rows:,} rows → {result.gold_path}\n"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
