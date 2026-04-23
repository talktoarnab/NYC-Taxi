"""
Configuration for the NYC Taxi ETL: data URLs, physical/financial thresholds, and paths.

`base_dir` defaults to the repository root (parent of the `nyc_taxi` package). Override
`Config(base_dir=...)` if you need to run outputs elsewhere.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# Resolve project root from this file: …/NYC Taxi/nyc_taxi/config.py → …/NYC Taxi
PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Config:
    """
    Single source of truth for pipeline inputs and tunable business rules.

    Zone IDs 132 (JFK) and 138 (LaGuardia) match the published taxi_zone lookup.
    The airport surcharge is the TLC “airport access fee” used for validation checks.
    """

    jfk_zone_id: int = 132
    lga_zone_id: int = 138
    airport_surcharge: float = 1.75
    # Physics-style caps: drop impossible or clearly erroneous trips
    max_distance_mi: float = 100.0
    max_speed_mph: float = 80.0
    # Inclusive hour ranges for “rush” labeling and chart highlighting
    rush_am_start: int = 7
    rush_am_end: int = 9
    rush_pm_start: int = 16
    rush_pm_end: int = 19

    # Official TLC data lake (Parquet) and static zone → borough table
    parquet_url: str = (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-02.parquet"
    )
    zone_url: str = (
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    )

    base_dir: Path = field(default_factory=lambda: PROJECT_ROOT)
    # When set (e.g. flat GHA artifact: zip root has `gold/` + `kpi/`, not `output/gold/`), gold/kpi
    # are read from here instead of base_dir / "output" / ...
    artifact_output_root: Path | None = None

    @property
    def raw_dir(self) -> Path:
        return self.base_dir / "data" / "raw"

    @property
    def lookup_dir(self) -> Path:
        return self.base_dir / "data" / "lookup"

    @property
    def gold_dir(self) -> Path:
        if self.artifact_output_root is not None:
            return self.artifact_output_root / "gold"
        return self.base_dir / "output" / "gold"

    @property
    def kpi_dir(self) -> Path:
        if self.artifact_output_root is not None:
            return self.artifact_output_root / "kpi"
        return self.base_dir / "output" / "kpi"

    @property
    def parquet_path(self) -> Path:
        return self.raw_dir / self.parquet_url.rsplit("/", 1)[-1]

    @property
    def zone_path(self) -> Path:
        return self.lookup_dir / "taxi_zone_lookup.csv"

    @property
    def gold_path(self) -> Path:
        return self.gold_dir / "nyc_taxi_gold.parquet"

    def ensure_dirs(self) -> None:
        """Create `data/*` and `output/*` folders before download or write."""
        for p in (self.raw_dir, self.lookup_dir, self.gold_dir, self.kpi_dir):
            p.mkdir(parents=True, exist_ok=True)


def data_period_label_from_gold_df(df) -> str:
    """
    Build a display label for chart / UI text from the Gold table's
    ``tpep_pickup_datetime`` (e.g. ``Feb 2026`` for a single month).
    """
    import pandas as pd

    col = "tpep_pickup_datetime"
    if col not in df.columns or len(df) == 0:
        return "—"
    s = pd.to_datetime(df[col], utc=True, errors="coerce").dropna()
    if s.empty:
        return "—"
    mn, mx = s.min(), s.max()
    if mn.year == mx.year and mn.month == mx.month:
        return mn.strftime("%b %Y")
    if mn.year == mx.year:
        return f"{mn.strftime('%b')}–{mx.strftime('%b %Y')}"
    return f"{mn.strftime('%b %Y')}–{mx.strftime('%b %Y')}"


# TLC `payment_type` integer codes (used for labels and payment-mix KPIs)
PAYMENT_MAP: dict[int, str] = {
    1: "Credit Card",
    2: "Cash",
    3: "No Charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided Trip",
}

default_config = Config()

# Headless chart generation for `python -m nyc_taxi` (no GUI required)
os.environ.setdefault("MPLBACKEND", "Agg")
