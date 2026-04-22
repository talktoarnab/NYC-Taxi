"""
Streamlit web UI: reads Gold + KPI data from a **GitHub Actions artifact** (recommended for
Streamlit Community Cloud) or, if unset, from local ``output/`` in the project tree.

**GitHub artifact mode** (set one of: env vars or ``.streamlit/secrets.toml``)

- ``NYC_TAXI_GH_TOKEN`` / ``GITHUB_TOKEN`` — PAT with **Actions: Read** (and ``repo`` for private repos)
- ``NYC_TAXI_GH_REPO`` / ``GITHUB_REPO`` — ``owner/repo`` (e.g. ``myuser/nyc-taxi``)
- ``GHA_ARTIFACT_NAME`` (optional) — must match the workflow `upload-artifact` name (default ``etl-output``)
- ``NYC_TAXI_ARTIFACT_CACHE`` (optional) — directory to cache downloaded zips (default ``~/.cache/nyc_taxi_streamlit``)

**Local mode** — omit the token/repo; the app uses ``Config.base_dir`` (project root) and
expects ``output/gold/nyc_taxi_gold.parquet`` from a prior ``python -m nyc_taxi`` run.
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

from nyc_taxi.config import Config, default_config
from nyc_taxi.github_artifact import download_and_extract_latest_artifact, parse_repo
from nyc_taxi.pipeline import run_pipeline

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
    """
    Return (token, owner/repo string, artifact_name) if GHA mode is enabled.

    Precedence: environment variables, then Streamlit ``secrets.toml``.
    """
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


@st.cache_data(
    ttl=300,
    show_spinner="Getting Latest Updates…",
)
def _load_base_dir_from_artifact(
    token: str,
    repo_full: str,
    artifact_name: str,
) -> tuple[str, str | None, int | str, str]:
    """
    Return ``(base_dir, artifact_output_root or None, artifact_id, created_at)``.

    Caches 5 minutes; use **Refresh** to call ``.clear()`` and pull a new run.
    """
    owner, repo = parse_repo(repo_full)
    base, aor, meta = download_and_extract_latest_artifact(
        token, owner, repo, artifact_name, cache_root=None
    )
    aid = meta.get("id", "—")
    aor_s: str | None = str(aor) if aor is not None else None
    return str(base), aor_s, aid, str(meta.get("created_at", ""))


def get_app_config() -> Config:
    """Config pointing at local ``output/`` or at extracted artifact layout."""
    trio = get_artifact_credentials()
    if trio is None:
        return default_config
    token, repo_full, aname = trio
    try:
        base_s, aor_s, aid, created = _load_base_dir_from_artifact(token, repo_full, aname)
        st.session_state["gha_artifact_id"] = aid
        st.session_state["gha_artifact_created"] = created
    except Exception as e:
        st.error(
            f"Failed to load data from GitHub artifact **{aname}**: {e!s}. "
            "Check token permissions (Actions: Read), `GITHUB_REPO`, and that a run produced the artifact."
        )
        st.stop()
    aor_p = Path(aor_s) if aor_s else None
    return Config(base_dir=Path(base_s), artifact_output_root=aor_p)


config = get_app_config()
gold_path = config.gold_path
kpi_dir = config.kpi_dir
artifact_mode = get_artifact_credentials() is not None

st.title("NYC Yellow Taxi — Analytics")
if artifact_mode:
    st.caption(
        "Latest data from the most recently downloaded **GitHub Actions** artifact (not from "
        "`NYC_TAXI_PARQUET_URL`). After changing that variable, re-run the ETL workflow and use "
        "**Refresh** in the app so a new `etl-output` is pulled.  "
        f"Artifact: `{st.session_state.get('gha_artifact_created', '—')}`"
    )
else:
    st.caption("Gold and KPIs from local `output/` (run `python -m nyc_taxi` or use GHA + secrets for artifact mode).")



def load_kpi(name: str) -> pd.DataFrame | None:
    p = kpi_dir / f"{name}.csv"
    if p.exists() and p.stat().st_size > 0:
        return pd.read_csv(p, index_col=0)
    return None


if not gold_path.exists():
    if artifact_mode:
        st.error(
            f"No Gold Parquet at `{gold_path}`. The artifact may be empty, expired, or the zip layout changed. "
            "Expect either `output/gold/nyc_taxi_gold.parquet` or a flat `gold/nyc_taxi_gold.parquet` (typical for `upload-artifact` of `output/`). "
            "Confirm **ETL** succeeded and `GHA_ARTIFACT_NAME` matches the workflow (default `etl-output`)."
        )
    else:
        st.warning(
            "No Gold Parquet yet. **Pick one:**\n\n"
            "1. **This machine** — in the project folder, run: `python -m nyc_taxi` (creates `output/gold/…`), then refresh this app.\n\n"
            "2. **Streamlit Cloud / no local ETL** — in **App settings → Secrets**, add `GITHUB_TOKEN` and `GITHUB_REPO` "
            "(and optional `GHA_ARTIFACT_NAME`) so the app pulls the latest workflow artifact instead of the repo. "
            "See `docs/DEPLOYMENT.md` and `.streamlit/secrets.toml.example`."
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
tab1, tab2, tab3, tab4 = st.tabs(
    ["Busiest hour", "Revenue by borough", "Efficiency ($/mi)", "Payment mix"]
)

with tab1:
    d = load_kpi("kpi_busiest_hour")
    if d is not None and "trip_count" in d.columns:
        st.bar_chart(d["trip_count"])
    else:
        st.caption("Missing kpi_busiest_hour.csv in artifact output.")
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
        st.caption("Missing kpi_revenue_by_borough.csv in artifact output.")
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
        st.caption("Missing kpi_efficiency_index.csv in artifact output.")
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
        st.caption("Missing kpi_payment_trends.csv in artifact output.")
    img = kpi_dir / "kpi_payment_trends.png"
    if img.exists():
        st.image(str(img), use_container_width=True)

st.divider()
st.caption(
    f"Active data: `{config.gold_path}` (base_dir=`{config.base_dir}`) · app root=`{_ROOT}`"
)
