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
if artifact_mode:
    with st.sidebar:
        if st.button("Reload from GitHub"):
            st.rerun()


def load_kpi(name: str) -> pd.DataFrame | None:
    p = kpi_dir / f"{name}.csv"
    if p.exists() and p.stat().st_size > 0:
        return pd.read_csv(p, index_col=0)
    return None


def _read_build_period(config: Config, df: pd.DataFrame) -> str:
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
    return data_period_for_chart_titles(config, df)


if not gold_path.exists():
    if artifact_mode:
        st.error(f"Gold table not found at `{gold_path}`.")
    else:
        st.warning("No Gold data. Run `python -m nyc_taxi` locally or configure GitHub artifact secrets.")
    st.stop()

df = pd.read_parquet(gold_path, engine="pyarrow")
st.caption(f"Period: **{_read_build_period(config, df)}**")

st.subheader("Dataset snapshot")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Rows", f"{len(df):,}")
c2.metric("Columns", f"{len(df.columns)}")
c3.metric("Avg total ($)", f"{df['total_amount'].mean():.2f}")
c4.metric("Avg $/mile", f"{df['revenue_per_mile'].mean():.2f}")

st.dataframe(
    df[
        [
            "tpep_pickup_datetime",
            "pickup_borough",
            "dropoff_borough",
            "trip_distance",
            "total_amount",
            "revenue_per_mile",
            "payment_label",
        ]
    ].head(200),
    use_container_width=True,
    height=280,
)

st.divider()
tab1, tab2, tab3, tab4 = st.tabs(
    ["Busiest hour", "Revenue by borough", "Efficiency ($/mi)", "Payment mix"]
)

with tab1:
    d = load_kpi("kpi_busiest_hour")
    if d is not None and "trip_count" in d.columns:
        st.bar_chart(d["trip_count"])
    img = kpi_dir / "kpi_busiest_hour.png"
    if img.exists():
        st.image(str(img), use_container_width=True)

with tab2:
    d = load_kpi("kpi_revenue_by_borough")
    if d is not None:
        st.dataframe(
            d.sort_values("total_revenue", ascending=False),
            use_container_width=True,
        )
    img = kpi_dir / "kpi_revenue_by_borough.png"
    if img.exists():
        st.image(str(img), use_container_width=True)

with tab3:
    d = load_kpi("kpi_efficiency_index")
    if d is not None:
        d = d.reset_index() if "hour_of_day" not in d.columns else d
    if d is not None and "hour_of_day" in d.columns:
        st.line_chart(
            d.set_index("hour_of_day")[["avg_rev_per_mile", "median_rev_per_mile"]]
        )
    img = kpi_dir / "kpi_efficiency_index.png"
    if img.exists():
        st.image(str(img), use_container_width=True)

with tab4:
    d = load_kpi("kpi_payment_trends")
    if d is not None and not d.empty and "payment_label" not in d.columns:
        d = d.reset_index()
    if d is not None and not d.empty and "share_pct" in d.columns:
        st.dataframe(d, use_container_width=True)
        st.bar_chart(d.set_index("payment_label" if "payment_label" in d.columns else d.columns[0])["share_pct"])
    img = kpi_dir / "kpi_payment_trends.png"
    if img.exists():
        st.image(str(img), use_container_width=True)
