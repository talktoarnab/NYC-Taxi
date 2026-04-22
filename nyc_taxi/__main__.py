"""
CLI entry point for the ETL pipeline.

Run from the project root so `nyc_taxi` resolves as a package::

    python -m nyc_taxi
    python -m nyc_taxi --no-charts -q
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import replace

from nyc_taxi.config import (
    TLC_TRIP_DATA_BASE,
    default_config,
)
from nyc_taxi.pipeline import run_pipeline


def main() -> int:
    """Parse arguments and run :func:`nyc_taxi.pipeline.run_pipeline`."""
    p = argparse.ArgumentParser(
        description="NYC TLC Yellow Taxi ETL: download, clean, Gold Parquet, KPI CSV/PNG."
    )
    p.add_argument(
        "--ym",
        metavar="YYYY-MM",
        help=(
            "TLC month for Yellow Taxi Parquet (e.g. 2025-10). "
            "Builds the standard CloudFront URL; overrides NYC_TAXI_PARQUET_URL."
        ),
    )
    p.add_argument(
        "--parquet-url",
        help=(
            "Full URL to a yellow_tripdata_*.parquet file; "
            "overrides --ym and NYC_TAXI_PARQUET_URL."
        ),
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
    if args.parquet_url and args.ym:
        p.error("Use either --parquet-url or --ym, not both.")
    cfg = default_config
    if args.parquet_url:
        cfg = replace(cfg, parquet_url=args.parquet_url.strip())
    elif args.ym:
        if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", args.ym):
            p.error("--ym must look like YYYY-MM (e.g. 2025-03).")
        month_url = f"{TLC_TRIP_DATA_BASE}yellow_tripdata_{args.ym}.parquet"
        cfg = replace(cfg, parquet_url=month_url)
    result = run_pipeline(
        cfg,
        verbose=not args.quiet,
        skip_charts=args.no_charts,
        apply_env_parquet_url=not (bool(args.ym) or bool(args.parquet_url)),
    )
    if not args.quiet:
        print(
            f"\nDone. Gold: {result.gold_rows:,} rows → {result.gold_path}\n"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
