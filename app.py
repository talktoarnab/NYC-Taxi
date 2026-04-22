"""
Streamlit web UI for NYC Yellow Taxi Gold data and KPI artifacts.

**Usage**

1. (Optional) Build data first: ``python -m nyc_taxi`` from the project root.
2. Start the app: ``streamlit run app.py``
3. Or use the in-app **Run full ETL pipeline** button (same pipeline as the CLI;
   may download ~50MB on first run).

The UI reads ``output/gold/nyc_taxi_gold.parquet`` and files under ``output/kpi/``.
If Gold is missing, the app shows a warning until you run the pipeline.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure `import nyc_taxi` works when Streamlit runs this file as a script
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd
import streamlit as st

from nyc_taxi.config import default_config
from nyc_taxi.pipeline import run_pipeline

st.set_page_config(
    page_title="NYC Taxi — Mobility & Revenue",
    layout="wide",
)

config = default_config
gold_path = config.gold_path
kpi_dir = config.kpi_dir

st.title("NYC Yellow Taxi — Analytics")
st.caption("Gold dataset and KPIs from the TLC ETL pipeline (Jan 2024 sample).")

# Primary action: full end-to-end run (downloads if needed, writes Gold + KPI CSV/PNG)
col_a, col_b = st.columns(2)
with col_a:
    if st.button("Run full ETL pipeline", type="primary", use_container_width=True):
        with st.spinner("Downloading, cleaning, and building KPIs…"):
            res = run_pipeline(config, verbose=False, skip_charts=False)
        st.success(
            f"Complete: {res.gold_rows:,} gold rows. "
            f"Flow: {res.raw_rows:,} → {res.after_physics:,} (physics) → "
            f"{res.after_financial:,} (financial)."
        )
        st.rerun()

with col_b:
    st.info(
        "CLI: `python -m nyc_taxi` from the project directory. "
        "First run downloads ~50MB Parquet and the zone lookup CSV."
    )


def load_kpi(name: str) -> pd.DataFrame | None:
    """Load a KPI table saved by the pipeline; first column is the index in the CSV file."""
    p = kpi_dir / f"{name}.csv"
    if p.exists() and p.stat().st_size > 0:
        return pd.read_csv(p, index_col=0)
    return None


# Gold Parquet is required for the main view; pipeline must run at least once
if not gold_path.exists():
    st.warning(
        "No Gold file yet. Click **Run full ETL pipeline** above, or run `python -m nyc_taxi`."
    )
    st.stop()

df = pd.read_parquet(gold_path, engine="pyarrow")
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
# Each tab: interactive chart from CSV (Streamlit) plus static PNG from the pipeline
tab1, tab2, tab3, tab4 = st.tabs(
    ["Busiest hour", "Revenue by borough", "Efficiency ($/mi)", "Payment mix"]
)

with tab1:
    d = load_kpi("kpi_busiest_hour")
    if d is not None and "trip_count" in d.columns:
        st.bar_chart(d["trip_count"])
    else:
        st.caption("Run the pipeline to generate kpi_busiest_hour.csv")
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
    else:
        st.caption("Run the pipeline to generate kpi_revenue_by_borough.csv")
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
    else:
        st.caption("Run the pipeline to generate kpi_efficiency_index.csv")
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
    else:
        st.caption("Run the pipeline to generate kpi_payment_trends.csv")
    img = kpi_dir / "kpi_payment_trends.png"
    if img.exists():
        st.image(str(img), use_container_width=True)

st.divider()
st.caption(f"Output paths: {config.gold_dir} · {config.kpi_dir} — project: `{_ROOT}`")
