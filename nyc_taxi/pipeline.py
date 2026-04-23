"""
NYC TLC Yellow Taxi ETL pipeline.

Flow: extract Parquet + zone lookup, apply physics-based row filters (NumPy masks),
financial checks (Pandas), feature engineering, write Gold Parquet, then emit KPI
CSVs and static chart images for reporting or the Streamlit app.
"""
from __future__ import annotations

import ssl
import urllib.request
import warnings
from dataclasses import dataclass
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker as mticker  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402

from nyc_taxi.config import (
    PAYMENT_MAP,
    Config,
    data_period_for_chart_titles,
    default_config,
    parquet_urls_from_repository_template,
    read_parquet_history_months,
)

warnings.filterwarnings("ignore")


@dataclass
class PipelineResult:
    """Row counts at key stages and path to the written Gold Parquet file."""

    raw_rows: int
    after_physics: int
    after_financial: int
    gold_rows: int
    gold_path: Path


def _download_if_missing(url: str, dest: Path, verbose: bool = True) -> None:
    """
    Stream the URL to disk if the file is absent.

    Primary path uses `requests` + `certifi` (works when the system SSL store
    is misconfigured, common on some macOS Python installs). On failure, falls
    back to `urllib` with a certifi-backed SSL context, then the default context.
    """
    if dest.exists():
        if verbose:
            mb = dest.stat().st_size / 1_048_576
            print(f"  [SKIP]  {dest.name:45s}  ({mb:.1f} MB already on disk)")
        return
    if verbose:
        print(f"  [DOWN]  {dest.name:45s}  downloading …", end="", flush=True)
    dest.parent.mkdir(parents=True, exist_ok=True)

    def _stream_requests() -> None:
        import certifi
        import requests

        with requests.get(url, stream=True, timeout=600, verify=certifi.where()) as r:
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        f.write(chunk)

    try:
        _stream_requests()
    except Exception:
        # Fallback: urllib with explicit CA bundle, then system defaults
        try:
            import certifi

            ctx = ssl.create_default_context(cafile=certifi.where())
        except Exception:
            ctx = ssl.create_default_context()
        with urllib.request.urlopen(url, context=ctx, timeout=600) as resp, open(
            dest, "wb"
        ) as out:
            while True:
                block = resp.read(1 << 20)
                if not block:
                    break
                out.write(block)

    if verbose:
        mb = dest.stat().st_size / 1_048_576
        print(f"  done  ({mb:.1f} MB)")


def run_pipeline(
    config: Config = default_config,
    verbose: bool = False,
    skip_charts: bool = False,
) -> PipelineResult:
    """
    Run the full ETL: download → load → physics filter → financial audit →
    features → Gold Parquet → KPI tables (CSVs) and optional chart PNGs.

    Args:
        config: URLs, paths, and thresholds; see `nyc_taxi.config.Config`.
        verbose: When True, print download progress and filter audit details.
        skip_charts: When True, skip matplotlib PNGs (KPI CSVs are still written).
    """
    config.ensure_dirs()
    plt.rcParams.update({"figure.dpi": 130, "figure.figsize": (10, 5)})
    sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)

    if verbose:
        print("Fetching data sources:")
    _download_if_missing(config.zone_url, config.zone_path, verbose)

    multi_urls = parquet_urls_from_repository_template()
    if multi_urls:
        if verbose:
            print(
                f"PARQUET_URL mode: PARQUET_HISTORY_MONTHS={read_parquet_history_months()} "
                f"→ {len(multi_urls)} monthly Parquet file(s) …"
            )
        frames: list[pd.DataFrame] = []
        for url in multi_urls:
            dest = config.raw_dir / url.rsplit("/", 1)[-1]
            try:
                _download_if_missing(url, dest, verbose)
                frames.append(pd.read_parquet(dest, engine="pyarrow"))
            except Exception as e:
                if dest.exists():
                    try:
                        dest.unlink()
                    except OSError:
                        pass
                if verbose:
                    print(f"  [WARN]  {dest.name}: {e!s} — skipped")
        if not frames:
            raise RuntimeError(
                "No Parquet months could be loaded. Check PARQUET_URL, network, and TLC "
                "availability for your date range."
            )
        df_raw = pd.concat(frames, ignore_index=True)
        del frames
    else:
        _download_if_missing(config.parquet_url, config.parquet_path, verbose)
        df_raw = pd.read_parquet(config.parquet_path, engine="pyarrow")

    if verbose:
        print("\nAll sources ready.")

    zone_lookup = pd.read_csv(config.zone_path)

    if verbose:
        print(
            f"\nRaw trip records  : {df_raw.shape[0]:,} rows  ×  {df_raw.shape[1]} columns"
        )
        print(f"Zone lookup table : {zone_lookup.shape[0]} rows  ×  {zone_lookup.shape[1]} columns")
    if (
        verbose
        and "tpep_pickup_datetime" in df_raw.columns
        and not df_raw.empty
    ):
        ts = pd.to_datetime(df_raw["tpep_pickup_datetime"], utc=True, errors="coerce")
        print(
            f"  Pickup dates (min … max, UTC): {ts.min()} … {ts.max()}"
        )

    df = df_raw.copy()
    raw_count = len(df)

    # --- Physics filter (vectorized; trip_distance and duration in consistent units) ---
    df["duration_hrs"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 3600

    td = df["trip_distance"].to_numpy()
    mask_distance = np.logical_and(td > 0, td <= config.max_distance_mi)

    # Speed = miles / hours; use NaN duration so invalid rows fail the speed check
    safe_duration = np.where(
        df["duration_hrs"].to_numpy() > 0, df["duration_hrs"].to_numpy(), np.nan
    )
    avg_speed = td / safe_duration
    mask_speed = avg_speed <= config.max_speed_mph

    # Same as dropoff > pickup: positive trip length in time
    mask_time = df["duration_hrs"].to_numpy() > 0
    combined_mask = np.logical_and.reduce([mask_distance, mask_speed, mask_time])
    df = df.loc[combined_mask].copy()
    after_physics = len(df)

    if verbose:
        print("\n── Physics Filter Audit ─────────────────────────────────────────────")
        print(f"  Raw rows             : {raw_count:>10,}")
        print(f"  Removed (distance)   : {(~mask_distance).sum():>10,}  ({(~mask_distance).mean()*100:.2f}%)")
        print(f"  Removed (speed)      : {(~mask_speed).sum():>10,}  ({(~mask_speed).mean()*100:.2f}%)")
        print(f"  Removed (time logic) : {(~mask_time).sum():>10,}  ({(~mask_time).mean()*100:.2f}%)")
        print(f"  Removed (combined)   : {raw_count - after_physics:>10,}  ({(raw_count - after_physics)/raw_count*100:.2f}%)")
        print(f"  Remaining rows       : {after_physics:>10,}")

    # --- Financial audit: non-negative money fields; tips and airport fee checks below ---
    neg_fare_mask = (df["fare_amount"] >= 0) & (df["total_amount"] >= 0)
    n_neg = (~neg_fare_mask).sum()
    df = df.loc[neg_fare_mask].copy()
    if verbose:
        print(f"\nNegative-fare rows removed : {n_neg:,}")
        print(f"Rows after fare filter     : {len(df):,}")

    # Tip as % of fare; avoid divide-by-zero on zero-fare rows
    df["tip_pct"] = np.where(
        df["fare_amount"] > 0,
        (df["tip_amount"] / df["fare_amount"]) * 100,
        0.0,
    )

    # TLC column name varies; JFK / LaGuardia use LocationID 132 and 138 in the lookup table
    fee_col = "Airport_fee" if "Airport_fee" in df.columns else "airport_fee"
    airport_zones = np.array([config.jfk_zone_id, config.lga_zone_id])
    is_airport_dest = np.isin(df["DOLocationID"].to_numpy(), airport_zones)

    if fee_col in df.columns:
        # Flag rows ending at an airport: recorded fee should meet TLC surcharge threshold
        df["airport_fee_valid"] = np.where(
            is_airport_dest,
            df[fee_col].to_numpy() >= config.airport_surcharge,
            True,
        )
        total_airport = int(is_airport_dest.sum())
        discrepant = int((~df.loc[is_airport_dest, "airport_fee_valid"]).sum())
        if verbose:
            print("\n── Airport Surcharge Audit ──────────────────────────────────────────")
            print(f"  Trips destined for JFK/LGA : {total_airport:>10,}")
            pct = (discrepant / total_airport * 100) if total_airport else 0.0
            print(f"  Fee discrepancies found    : {discrepant:>10,}  ({pct:.2f}% of airport trips)")
    else:
        df["airport_fee_valid"] = True
        if verbose:
            print(f'\nColumn "{fee_col}" not present — skipping surcharge audit.')

    after_financial = len(df)
    if verbose:
        print(f"\nRows after financial audit : {after_financial:,}")

    # --- Feature engineering: time-of-day, rush flag, zone → borough, efficiency ---
    df["hour_of_day"] = df["tpep_pickup_datetime"].dt.hour
    df["day_of_week"] = df["tpep_pickup_datetime"].dt.day_name()
    df["month"] = df["tpep_pickup_datetime"].dt.month
    df["date"] = df["tpep_pickup_datetime"].dt.date

    hour_arr = df["hour_of_day"].to_numpy()
    # Morning 7–9 and evening 16–19 (inclusive) — config-driven for KPI shading
    df["is_rush_hour"] = np.where(
        ((hour_arr >= config.rush_am_start) & (hour_arr <= config.rush_am_end))
        | ((hour_arr >= config.rush_pm_start) & (hour_arr <= config.rush_pm_end)),
        True,
        False,
    )

    # Map TLC LocationID to human-readable borough / zone (pickup and dropoff)
    zone_pu = zone_lookup[["LocationID", "Borough", "Zone"]].rename(
        columns={
            "LocationID": "PULocationID",
            "Borough": "pickup_borough",
            "Zone": "pickup_zone",
        }
    )
    zone_do = zone_lookup[["LocationID", "Borough", "Zone"]].rename(
        columns={
            "LocationID": "DOLocationID",
            "Borough": "dropoff_borough",
            "Zone": "dropoff_zone",
        }
    )
    df = df.merge(zone_pu, on="PULocationID", how="left").merge(
        zone_do, on="DOLocationID", how="left"
    )

    # Driver-side efficiency: total collected per mile driven
    df["revenue_per_mile"] = np.where(
        df["trip_distance"] > 0,
        df["total_amount"] / df["trip_distance"],
        np.nan,
    )
    df["payment_label"] = df["payment_type"].map(PAYMENT_MAP).fillna("Unknown")

    # --- Load: single Gold table for analytics and the Streamlit app ---
    df_gold = df.copy()
    df_gold.to_parquet(config.gold_path, index=False, engine="pyarrow")
    gold_mb = config.gold_path.stat().st_size / 1_048_576

    if verbose:
        print("\n════════════════════════════════════════════════════════════════════")
        print("                    FILTER STAGE AUDIT LOG                          ")
        print("════════════════════════════════════════════════════════════════════")
        print(f"  Stage 0 — Raw ingestion        : {raw_count:>10,} rows  (100.00%)")
        print(
            f"  Stage 1 — Physics filter       : {after_physics:>10,} rows  "
            f"({after_physics/raw_count*100:.2f}%)  [-{raw_count - after_physics:,} removed]"
        )
        print(
            f"  Stage 2 — Financial audit      : {after_financial:>10,} rows  "
            f"({after_financial/raw_count*100:.2f}%)  [-{after_physics - after_financial:,} removed]"
        )
        print(
            f"  Stage 3 — Gold dataset (final) : {len(df_gold):>10,} rows  "
            f"({len(df_gold)/raw_count*100:.2f}%)"
        )
        print("════════════════════════════════════════════════════════════════════")
        print(f"  Gold file written to : {config.gold_path}")
        print(f"  Gold file size       : {gold_mb:.1f} MB")
        print("════════════════════════════════════════════════════════════════════")

    kpi_dir = config.kpi_dir

    def save_kpi(frame: pd.DataFrame, name: str) -> Path:
        """Write one KPI table next to the chart PNGs (stable filenames for the UI)."""
        path = kpi_dir / f"{name}.csv"
        frame.to_csv(path)
        if verbose:
            print(f"  Saved: {path}")
        return path

    # --- Analytical load: four KPI tables aligned with the plan / notebook ---
    kpi_busiest_hour = (
        df_gold.groupby("hour_of_day")
        .size()
        .reset_index(name="trip_count")
        .sort_values("hour_of_day")
    )
    save_kpi(kpi_busiest_hour.set_index("hour_of_day"), "kpi_busiest_hour")

    kpi_borough_revenue = (
        df_gold.groupby("pickup_borough")["total_amount"]
        .agg(total_revenue="sum", avg_revenue="mean", trip_count="count")
        .reset_index()
        .sort_values("total_revenue", ascending=True)
    )
    save_kpi(kpi_borough_revenue.set_index("pickup_borough"), "kpi_revenue_by_borough")

    kpi_efficiency = (
        df_gold.groupby("hour_of_day")["revenue_per_mile"]
        .agg(
            avg_rev_per_mile="mean",
            median_rev_per_mile="median",
            trip_count="count",
        )
        .reset_index()
        .sort_values("hour_of_day")
    )
    save_kpi(kpi_efficiency.set_index("hour_of_day"), "kpi_efficiency_index")

    kpi_payment = (
        df_gold.groupby("payment_label")
        .agg(
            trip_count=("payment_label", "count"),
            total_revenue=("total_amount", "sum"),
            avg_tip_pct=("tip_pct", "mean"),
        )
        .reset_index()
    )
    kpi_payment["share_pct"] = (
        kpi_payment["trip_count"] / kpi_payment["trip_count"].sum() * 100
    ).round(2)
    kpi_payment = kpi_payment.sort_values("trip_count", ascending=False)
    save_kpi(kpi_payment.set_index("payment_label"), "kpi_payment_trends")

    if not skip_charts:
        _save_all_charts(
            config,
            df_gold,
            kpi_busiest_hour,
            kpi_borough_revenue,
            kpi_efficiency,
            kpi_payment,
            verbose,
        )

    return PipelineResult(
        raw_rows=raw_count,
        after_physics=after_physics,
        after_financial=after_financial,
        gold_rows=len(df_gold),
        gold_path=config.gold_path,
    )


def _save_all_charts(
    config: Config,
    df_gold: pd.DataFrame,
    kpi_busiest_hour: pd.DataFrame,
    kpi_borough_revenue: pd.DataFrame,
    kpi_efficiency: pd.DataFrame,
    kpi_payment: pd.DataFrame,
    verbose: bool,
) -> None:
    """
    Save matplotlib figures to `output/kpi/*.png` (no display; `Agg` backend).

    Mirrors the notebook visuals: demand by hour, borough revenue, $/mile by hour, payment mix.
    """
    kpi_dir = config.kpi_dir
    R = config
    period = data_period_for_chart_titles(config, df_gold)
    _period_fp = kpi_dir / "kpi_chart_period.txt"
    kpi_dir.mkdir(parents=True, exist_ok=True)
    _period_fp.write_text(period + "\n", encoding="utf-8")
    # Mirror at output/etl_build_period.txt so upload-artifact path: output/ always includes
    # one path the Streamlit artifact reader can find (flat or nested zip layouts).
    _out = R.base_dir / "output" / "etl_build_period.txt"
    _out.parent.mkdir(parents=True, exist_ok=True)
    _out.write_text(period + "\n", encoding="utf-8")
    if verbose:
        print(
            f"  KPI chart title period: {period!r}  (PNG → {kpi_dir.resolve()!s}/; "
            f"{_period_fp.name}; output/{_out.name})"
        )

    # KPI 1 — trip volume by hour (rush hours highlighted for readability)
    fig, ax = plt.subplots(figsize=(12, 5))
    colors = [
        "#e74c3c"
        if ((R.rush_am_start <= h <= R.rush_am_end) or (R.rush_pm_start <= h <= R.rush_pm_end))
        else "#3498db"
        for h in kpi_busiest_hour["hour_of_day"]
    ]
    ax.bar(
        kpi_busiest_hour["hour_of_day"],
        kpi_busiest_hour["trip_count"],
        color=colors,
        edgecolor="white",
        linewidth=0.5,
    )
    ax.set_xlabel("Hour of Day (24 h)", fontsize=12)
    ax.set_ylabel("Number of Trips", fontsize=12)
    ax.set_title(
        f"NYC Yellow Taxi — Busiest Hours ({period})",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xticks(range(0, 24))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}k"))
    legend_elements = [
        Patch(facecolor="#e74c3c", label="Rush Hour"),
        Patch(facecolor="#3498db", label="Off-Peak"),
    ]
    ax.legend(handles=legend_elements, loc="upper left")
    peak_hour = kpi_busiest_hour.loc[kpi_busiest_hour["trip_count"].idxmax(), "hour_of_day"]
    peak_count = kpi_busiest_hour["trip_count"].max()
    ax.annotate(
        f"Peak: {peak_hour:02d}:00\n({peak_count/1000:.1f}k trips)",
        xy=(peak_hour, peak_count),
        xytext=(peak_hour + 1.5, peak_count * 0.95),
        arrowprops=dict(arrowstyle="->", color="black"),
        fontsize=9,
    )
    plt.tight_layout()
    fig.savefig(kpi_dir / "kpi_busiest_hour.png", bbox_inches="tight")
    plt.close(fig)

    # KPI 2 — total and average revenue by pickup borough
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    palette = sns.color_palette("Blues_d", len(kpi_borough_revenue))
    bars = axes[0].barh(
        kpi_borough_revenue["pickup_borough"],
        kpi_borough_revenue["total_revenue"] / 1_000_000,
        color=palette,
    )
    axes[0].set_xlabel("Total Revenue (USD millions)", fontsize=11)
    axes[0].set_title("Total Revenue by Pickup Borough", fontsize=13, fontweight="bold")
    for bar, val in zip(bars, kpi_borough_revenue["total_revenue"] / 1_000_000):
        axes[0].text(
            val + 0.05,
            bar.get_y() + bar.get_height() / 2,
            f"${val:.1f}M",
            va="center",
            fontsize=9,
        )
    kpi_sorted_avg = kpi_borough_revenue.sort_values("avg_revenue", ascending=True)
    palette2 = sns.color_palette("Greens_d", len(kpi_sorted_avg))
    bars2 = axes[1].barh(
        kpi_sorted_avg["pickup_borough"],
        kpi_sorted_avg["avg_revenue"],
        color=palette2,
    )
    axes[1].set_xlabel("Average Revenue per Trip (USD)", fontsize=11)
    axes[1].set_title("Avg Revenue per Trip by Borough", fontsize=13, fontweight="bold")
    for bar, val in zip(bars2, kpi_sorted_avg["avg_revenue"]):
        axes[1].text(
            val + 0.2,
            bar.get_y() + bar.get_height() / 2,
            f"${val:.2f}",
            va="center",
            fontsize=9,
        )
    plt.suptitle(
        f"NYC Taxi — Driver Profitability by Borough ({period})",
        fontsize=14,
        fontweight="bold",
        y=1.01,
    )
    plt.tight_layout()
    fig.savefig(kpi_dir / "kpi_revenue_by_borough.png", bbox_inches="tight")
    plt.close(fig)

    # KPI 3 — mean and median revenue per mile by hour (rush windows shaded)
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(
        kpi_efficiency["hour_of_day"],
        kpi_efficiency["avg_rev_per_mile"],
        color="#2ecc71",
        linewidth=2.5,
        marker="o",
        markersize=5,
        label="Mean $/mile",
    )
    ax.plot(
        kpi_efficiency["hour_of_day"],
        kpi_efficiency["median_rev_per_mile"],
        color="#e67e22",
        linewidth=2,
        linestyle="--",
        marker="s",
        markersize=4,
        label="Median $/mile",
    )
    ax.axvspan(R.rush_am_start, R.rush_am_end, alpha=0.12, color="red", label="Rush hours")
    ax.axvspan(R.rush_pm_start, R.rush_pm_end, alpha=0.12, color="red")
    ax.set_xlabel("Hour of Day (24 h)", fontsize=12)
    ax.set_ylabel("Revenue per Mile (USD)", fontsize=12)
    ax.set_title(
        f"NYC Taxi — Efficiency Index: Revenue per Mile by Hour ({period})",
        fontsize=13,
        fontweight="bold",
    )
    ax.set_xticks(range(0, 24))
    ax.legend()
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("$%.2f"))
    plt.tight_layout()
    fig.savefig(kpi_dir / "kpi_efficiency_index.png", bbox_inches="tight")
    plt.close(fig)

    # KPI 4 — share of trips by payment type and average tip % where tips apply
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    pie_colors = ["#3498db", "#2ecc71", "#e74c3c", "#f39c12", "#9b59b6", "#95a5a6"]
    axes[0].pie(
        kpi_payment["trip_count"],
        labels=kpi_payment["payment_label"],
        autopct="%1.1f%%",
        startangle=140,
        colors=pie_colors[: len(kpi_payment)],
        pctdistance=0.82,
        wedgeprops=dict(linewidth=1.5, edgecolor="white"),
    )
    axes[0].set_title("Trip Share by Payment Method", fontsize=13, fontweight="bold")
    top_payment = kpi_payment[kpi_payment["avg_tip_pct"] > 0]
    bars = axes[1].bar(
        top_payment["payment_label"],
        top_payment["avg_tip_pct"],
        color=pie_colors[: len(top_payment)],
        edgecolor="white",
    )
    axes[1].set_ylabel("Average Tip (%)", fontsize=11)
    axes[1].set_title("Average Tip % by Payment Method", fontsize=13, fontweight="bold")
    axes[1].set_xlabel("Payment Method", fontsize=11)
    for bar, val in zip(bars, top_payment["avg_tip_pct"]):
        axes[1].text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.2,
            f"{val:.1f}%",
            ha="center",
            fontsize=9,
        )
    plt.suptitle(
        f"NYC Taxi — Payment Trends ({period})",
        fontsize=14,
        fontweight="bold",
        y=1.01,
    )
    plt.tight_layout()
    fig.savefig(kpi_dir / "kpi_payment_trends.png", bbox_inches="tight")
    plt.close(fig)

    if verbose:
        for name in (
            "kpi_busiest_hour.png",
            "kpi_revenue_by_borough.png",
            "kpi_efficiency_index.png",
            "kpi_payment_trends.png",
        ):
            print(f"  Chart: {kpi_dir / name}")
