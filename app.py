"""
Streamlit UI: Gold + KPIs from local ``output/`` or from the latest GitHub Actions
``etl-output`` artifact when ``GITHUB_TOKEN`` + ``GITHUB_REPO`` (or Streamlit secrets)
are set. Optional: ``GHA_ARTIFACT_NAME``, ``NYC_TAXI_ARTIFACT_CACHE``.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
import pyarrow.parquet as pq
import streamlit as st

from nyc_taxi.config import Config, data_period_for_chart_titles, default_config
from nyc_taxi.github_artifact import download_and_extract_latest_artifact, parse_repo

st.set_page_config(
    page_title="NYC Taxi — Mobility & Revenue",
    layout="wide",
)


def _streamlit_secrets() -> dict | None:
    try:
        return {k: st.secrets[k] for k in st.secrets}
    except Exception:
        return None


def get_artifact_credentials() -> tuple[str, str, str] | None:
    n = os.environ.get("GHA_ARTIFACT_NAME", "etl-output")
    t = os.environ.get("NYC_TAXI_GH_TOKEN")
    r = os.environ.get("NYC_TAXI_GH_REPO")
    if t and r:
        return t, r, n
    t = os.environ.get("GITHUB_TOKEN")
    r = os.environ.get("GITHUB_REPO")
    if t and r:
        return t, r, n

    sec = _streamlit_secrets()
    if not sec:
        return None
    t = (sec.get("NYC_TAXI_GH_TOKEN") or sec.get("GITHUB_TOKEN") or sec.get("github_token", "")) or ""
    r = (sec.get("NYC_TAXI_GH_REPO") or sec.get("GITHUB_REPO") or sec.get("github_repo", "")) or ""
    n = (sec.get("GHA_ARTIFACT_NAME") or sec.get("gha_artifact_name") or n) or "etl-output"
    if t and r:
        return t, r, str(n)
    return None


def _load_base_dir_from_artifact(
    token: str,
    repo_full: str,
    artifact_name: str,
) -> tuple[str, str | None]:
    owner, repo = parse_repo(repo_full)
    base, aor, _meta = download_and_extract_latest_artifact(
        token, owner, repo, artifact_name, cache_root=None
    )
    aor_s: str | None = str(aor) if aor is not None else None
    return str(base), aor_s


def get_app_config() -> Config:
    trio = get_artifact_credentials()
    if trio is None:
        return default_config
    token, repo_full, aname = trio
    try:
        base_s, aor_s = _load_base_dir_from_artifact(token, repo_full, aname)
    except Exception as e:
        st.error(f"Could not load artifact **{aname}**: {e!s}")
        st.stop()
    aor_p = Path(aor_s) if aor_s else None
    return Config(base_dir=Path(base_s), artifact_output_root=aor_p)


config = get_app_config()
gold_path = config.gold_path
kpi_dir = config.kpi_dir
artifact_mode = get_artifact_credentials() is not None

st.title("NYC Yellow Taxi — Analytics")


def load_kpi(name: str) -> pd.DataFrame | None:
    p = kpi_dir / f"{name}.csv"
    if p.exists() and p.stat().st_size > 0:
        return pd.read_csv(p, index_col=0)
    return None


def _read_build_period(config: Config, gold_path: Path) -> str:
    b = config.base_dir
    for path in (
        config.kpi_dir / "kpi_chart_period.txt",
        b / "etl_build_period.txt",
        b / "output" / "etl_build_period.txt",
    ):
        try:
            if path.is_file() and path.stat().st_size > 0:
                return path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
    for p in b.rglob("kpi_chart_period.txt"):
        try:
            if p.is_file() and p.stat().st_size > 0:
                return p.read_text(encoding="utf-8").strip()
        except (OSError, ValueError):
            continue
    ts = pd.read_parquet(
        gold_path, columns=["tpep_pickup_datetime"], engine="pyarrow"
    )
    return data_period_for_chart_titles(config, ts)


if not gold_path.exists():
    if artifact_mode:
        st.error(f"Gold table not found at `{gold_path}`.")
    else:
        st.warning("No Gold data. Run `python -m nyc_taxi` locally or configure GitHub artifact secrets.")
    st.stop()

st.caption(f"Period: **{_read_build_period(config, gold_path)}**")

n_trips = int(pq.ParquetFile(gold_path).metadata.num_rows)
money = pd.read_parquet(
    gold_path, columns=["total_amount", "revenue_per_mile"], engine="pyarrow"
)
avg_total = float(money["total_amount"].mean())
avg_rpm = float(money["revenue_per_mile"].mean())
del money

st.subheader("Summary")
c1, c2, c3 = st.columns(3)
c1.metric("Trips (Gold rows)", f"{n_trips:,}")
c2.metric("Avg fare + extras ($)", f"{avg_total:.2f}")
c3.metric("Avg revenue / mile ($)", f"{avg_rpm:.2f}")

borough_df = load_kpi("kpi_revenue_by_borough")
payment_df = load_kpi("kpi_payment_trends")
if borough_df is not None and not borough_df.empty and "total_revenue" in borough_df.columns:
    br = borough_df.sort_values("total_revenue", ascending=False)
    total_rev = float(br["total_revenue"].sum())
    st.markdown("**Revenue by pickup borough** (% of total)")
    n_show = min(5, len(br))
    cols = st.columns(n_show)
    for i, (bname, row) in enumerate(br.head(n_show).iterrows()):
        pct = (float(row["total_revenue"]) / total_rev * 100) if total_rev else 0.0
        cols[i].metric(str(bname), f"{pct:.1f}%", f"${float(row['total_revenue'])/1e6:.2f}M")

if payment_df is not None and not payment_df.empty:
    pay = (
        payment_df.reset_index()
        if "payment_label" not in payment_df.columns
        else payment_df.copy()
    )
    if "share_pct" in pay.columns and "payment_label" in pay.columns:
        st.markdown("**Payment method** (% of trips)")
        n_pay = min(6, len(pay))
        pc = st.columns(min(4, n_pay))
        for i, row in enumerate(pay.head(n_pay).itertuples(index=False)):
            pc[i % len(pc)].metric(
                str(row.payment_label),
                f"{float(row.share_pct):.1f}%",
                f"{int(row.trip_count):,} trips",
            )

st.divider()
st.subheader("Charts")

d_hour = load_kpi("kpi_busiest_hour")
if d_hour is not None and "trip_count" in d_hour.columns:
    st.markdown("##### Trips by hour")
    st.caption("Share of trips in each hour (bar height ∝ count).")
    st.bar_chart(d_hour["trip_count"])

d_borough = load_kpi("kpi_revenue_by_borough")
if d_borough is not None and "total_revenue" in d_borough.columns:
    st.markdown("##### Revenue by borough")
    st.caption("% of total revenue by pickup borough.")
    bb = d_borough.sort_values("total_revenue", ascending=False)
    tr = float(bb["total_revenue"].sum())
    pct_series = bb["total_revenue"] / tr * 100 if tr else bb["total_revenue"] * 0
    st.bar_chart(pct_series.rename("% revenue"))

d_eff = load_kpi("kpi_efficiency_index")
if d_eff is not None:
    d_eff = d_eff.reset_index() if "hour_of_day" not in d_eff.columns else d_eff
if d_eff is not None and "hour_of_day" in d_eff.columns:
    st.markdown("##### Revenue per mile by hour")
    st.caption("Mean and median $/mile (multi-month ETL may show mean only for both lines).")
    st.line_chart(
        d_eff.set_index("hour_of_day")[["avg_rev_per_mile", "median_rev_per_mile"]]
    )

d_pay = load_kpi("kpi_payment_trends")
if d_pay is not None and not d_pay.empty:
    d_pay = d_pay.reset_index() if "payment_label" not in d_pay.columns else d_pay
if d_pay is not None and not d_pay.empty and "share_pct" in d_pay.columns:
    st.markdown("##### Payment mix")
    st.caption("% of trips by payment type.")
    label_col = "payment_label" if "payment_label" in d_pay.columns else d_pay.columns[0]
    st.bar_chart(d_pay.set_index(label_col)["share_pct"])

for label, fname in (
    ("Busiest hour (detail)", "kpi_busiest_hour.png"),
    ("Revenue by borough (detail)", "kpi_revenue_by_borough.png"),
    ("Efficiency (detail)", "kpi_efficiency_index.png"),
    ("Payment mix (detail)", "kpi_payment_trends.png"),
):
    img = kpi_dir / fname
    if img.exists():
        with st.expander(label):
            st.image(str(img), use_container_width=True)
