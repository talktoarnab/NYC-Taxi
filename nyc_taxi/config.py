"""
Configuration for the NYC Taxi ETL: data URLs, physical/financial thresholds, and paths.

`base_dir` defaults to the repository root (parent of the `nyc_taxi` package). Override
`Config(base_dir=...)` if you need to run outputs elsewhere.

Set env **PARQUET_URL** to any TLC ``yellow_tripdata_YYYY-MM.parquet`` URL; the ETL will
rewrite the month segment and download **PARQUET_HISTORY_MONTHS** (env / repo variable; default **60**)
of Yellow Taxi Parquet files, then consolidate into one Gold table. Unset **PARQUET_URL**
to keep single-file mode using :attr:`Config.parquet_url`.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

# TLC monthly Yellow trip files (used when expanding PARQUET_URL).
_TRIP_PQ_FILENAME = re.compile(
    r"(?i)(yellow_tripdata_)(\d{4})-(\d{2})(\.parquet)"
)
PARQUET_URL_ENV = "PARQUET_URL"
PARQUET_HISTORY_MONTHS_ENV = "PARQUET_HISTORY_MONTHS"
DEFAULT_PARQUET_HISTORY_MONTHS = 60
MAX_PARQUET_HISTORY_MONTHS = 240

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


def data_period_from_parquet_url(url: str) -> str | None:
    """
    Parse ``.../yellow_tripdata_YYYY-MM.parquet`` from the TLC trip URL, if present.
    Used so chart titles match the configured file month, not a stale or mis-parsed
    timestamp column in Gold.
    """
    m = re.search(
        r"yellow_tripdata_(\d{4})-(\d{2})\.parquet",
        url,
        re.IGNORECASE,
    )
    if not m:
        return None
    y, mo = int(m.group(1)), int(m.group(2))
    if not 1 <= mo <= 12:
        return None
    return date(y, mo, 1).strftime("%b %Y")


def data_period_for_chart_titles(config: Config, df) -> str:
    """
    Chart / UI label from Gold trip timestamps when available (single month or span);
    otherwise fall back to ``parquet_url`` filename.
    """
    label = data_period_label_from_gold_df(df)
    if label != "—":
        return label
    if config.artifact_output_root is not None:
        return "—"
    u = data_period_from_parquet_url(getattr(config, "parquet_url", "") or "")
    return u if u else "—"


def build_yellow_trip_parquet_url(template: str, year: int, month: int) -> str:
    """Replace ``yellow_tripdata_YYYY-MM.parquet`` in *template* with the given month."""
    if not _TRIP_PQ_FILENAME.search(template):
        raise ValueError(
            "PARQUET_URL must contain a TLC Yellow file segment like "
            "'yellow_tripdata_2026-02.parquet'"
        )
    if not 1 <= month <= 12:
        raise ValueError(f"month must be 1..12, got {month}")

    def _sub(m: re.Match[str]) -> str:
        return f"{m.group(1)}{year:04d}-{month:02d}{m.group(4)}"

    return _TRIP_PQ_FILENAME.sub(_sub, template, count=1)


def rolling_month_pairs(end: date | None, n_months: int) -> list[tuple[int, int]]:
    """The *n_months* most recent calendar months ending at *end* (inclusive), oldest first."""
    if n_months < 1:
        raise ValueError("n_months must be >= 1")
    if end is None:
        end = date.today()
    out: list[tuple[int, int]] = []
    y, m = end.year, end.month
    for _ in range(n_months):
        out.append((y, m))
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    out.reverse()
    return out


def read_parquet_history_months() -> int:
    """
    Month count for consolidated ETL (``PARQUET_HISTORY_MONTHS`` env).

    Set this in GitHub Actions from repository variable ``PARQUET_HISTORY_MONTHS``.
    Empty / unset → :data:`DEFAULT_PARQUET_HISTORY_MONTHS`.
    """
    raw = (os.environ.get(PARQUET_HISTORY_MONTHS_ENV) or "").strip()
    if not raw:
        return DEFAULT_PARQUET_HISTORY_MONTHS
    try:
        n = int(raw, 10)
    except ValueError as e:
        raise ValueError(
            f"{PARQUET_HISTORY_MONTHS_ENV} must be an integer, got {raw!r}"
        ) from e
    if n < 1:
        raise ValueError(f"{PARQUET_HISTORY_MONTHS_ENV} must be >= 1, got {n}")
    if n > MAX_PARQUET_HISTORY_MONTHS:
        raise ValueError(
            f"{PARQUET_HISTORY_MONTHS_ENV} must be <= {MAX_PARQUET_HISTORY_MONTHS}, got {n}"
        )
    return n


def parquet_urls_from_repository_template() -> list[str] | None:
    """
    If ``PARQUET_URL`` is set, return TLC URLs for the rolling history window; else ``None``.
    Window length comes from :func:`read_parquet_history_months`.
    """
    raw = (os.environ.get(PARQUET_URL_ENV) or "").strip()
    if not raw:
        return None
    n = read_parquet_history_months()
    pairs = rolling_month_pairs(date.today(), n)
    return [build_yellow_trip_parquet_url(raw, y, mo) for y, mo in pairs]


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
