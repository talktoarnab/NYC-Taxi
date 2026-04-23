# NYC Yellow Taxi ETL and Analytics

This project ingests **NYC TLC Yellow Taxi** trip data (Parquet), applies **physics-based** row filters and **financial checks**, enriches trips with time and **borough** labels, and produces a **Gold** dataset plus **KPI** tables and charts. A **Jupyter** notebook, a **CLI** program, and a **Streamlit** app share the same core logic in the `nyc_taxi` package.

## What it does

1. **Extract** — Downloads a sample month of Yellow Taxi Parquet and the [taxi zone lookup](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) (LocationID → Borough) from the TLC public bucket (skipped if files already exist under `data/`).
2. **Transform** — Vectorized **NumPy** masks for trip distance, implied speed, and time order; **Pandas** for negative fare removal, `tip_pct`, and airport-fee checks where applicable; merges for pickup/dropoff borough names.
3. **Load** — Writes `output/gold/nyc_taxi_gold.parquet` and KPI CSV/PNG files under `output/kpi/`.

## Project layout

| Path | Purpose |
|------|-----------|
| `nyc_taxi/config.py` | URLs, JFK/LGA zone IDs, distance/speed limits, rush-hour hours, and directory paths |
| `nyc_taxi/pipeline.py` | Full ETL, KPI aggregations, matplotlib chart export (non-interactive `Agg` backend) |
| `nyc_taxi/__main__.py` | `python -m nyc_taxi` CLI |
| `app.py` | Streamlit dashboard (local `output/` or latest **GitHub Actions** artifact via API) |
| `nyc_taxi/github_artifact.py` | Download & extract `etl-output` from the GitHub REST API |
| `nyc_taxi_pipeline.ipynb` | Step-by-step walkthrough of the same pipeline in a notebook |
| `data/raw/` | Downloaded Parquet (e.g. `yellow_tripdata_2024-01.parquet`) |
| `data/lookup/` | `taxi_zone_lookup.csv` |
| `output/gold/` | Refined Parquet for analysis |
| `output/kpi/` | `kpi_*.csv` and `kpi_*.png` (busiest hour, revenue by borough, efficiency, payment mix) |

## Requirements

- Python 3.10+ recommended  
- See `requirements.txt` (pandas, numpy, pyarrow, matplotlib, seaborn, requests, certifi, jupyter, streamlit)

Install:

```bash
pip install -r requirements.txt
```

`certifi` is used with `requests` for HTTPS downloads when the system certificate store is unreliable (common on some macOS Python installs).

## Run the pipeline (CLI)

From the **project root** (the folder that contains `nyc_taxi/` and `app.py`):

```bash
python -m nyc_taxi
```

Options:

- `--no-charts` — Write KPI **CSV** files only; skip PNG chart generation (faster).
- `-q` / `--quiet` — Minimal console output.
- `--ym YYYY-MM` — Use the standard TLC URL for that month (overrides `NYC_TAXI_PARQUET_URL`).
- `--parquet-url` — Use a full Parquet URL (overrides `--ym` and env).
- `--show-parquet-env` — Print how `NYC_TAXI_PARQUET_URL` is read (raw `repr`, whether the key exists, resolved URL) and exit; no ETL or download.

On success you get row-count audits in the console, a Gold Parquet file, and KPI files under `output/`.

## Run the web app (Streamlit)

```bash
streamlit run app.py
```

- **With GitHub Actions:** If you set `GITHUB_TOKEN` and `GITHUB_REPO` (or `NYC_TAXI_GH_*` env vars) in **Streamlit Cloud → Secrets** (or a local ``.streamlit/secrets.toml``), the app **downloads the latest `etl-output` artifact** (same layout as a local `output/` run) and caches it under `~/.cache/nyc_taxi_streamlit`. It does **not** read large Parquet from the git tree.
- **Local only:** Unset those secrets; the app uses the project’s `output/` and offers an optional **Run ETL locally** button, or run `python -m nyc_taxi` first.

Details: **[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)**, and the `app.py` module docstring.

## Run the notebook

```bash
jupyter notebook nyc_taxi_pipeline.ipynb
```

Execute cells in order. The notebook is self-contained (it can `pip install` missing packages in the first code cell).

## Programmatic use

```python
from nyc_taxi import run_pipeline, default_config, Config

# Custom output root
cfg = Config(base_dir="/path/to/project")
result = run_pipeline(cfg, verbose=True, skip_charts=False)
print(result.gold_path, result.gold_rows)
```

## Data sources (default)

- **Trips** — e.g. `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet` (default when `NYC_TAXI_PARQUET_URL` is unset). The download is saved under `data/raw/` using the filename from the URL.
- **Zones** — `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`

**Another month, on a schedule:** set the environment variable `NYC_TAXI_PARQUET_URL` to the full URL for the month you want (cron, systemd timer, or `python -m nyc_taxi --ym 2025-10`). For **GitHub Actions** only, add that name as a **Repository variable** (Settings → Actions → Variables) or a **Repository secret** (the workflow passes `secrets.NYC_TAXI_PARQUET_URL` first, then `vars.…`). `config.py` only reads the real OS environment at run time, not a variable stored only on GitHub’s UI until the workflow sets `env:`. The pipeline only downloads a file if it is missing—delete the old `data/raw/yellow_tripdata_*.parquet` (or use a new month so the path changes) when you need a fresh download.

## Deployment (Streamlit, GitHub Actions, OCI, AWS)

Step-by-step guides (Streamlit Community Cloud with ETL in GitHub Actions, Oracle Cloud VM, and AWS) are in **[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)**. The repository includes a sample **[`.github/workflows/etl.yml`](.github/workflows/etl.yml)** and a root **[`Dockerfile`](Dockerfile)** for container-based hosts.

## License and data terms

Trip data and zone shapes are **public NYC TLC** material; follow [NYC’s open data / TLC terms of use](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) for redistribution and attribution. This repository contains code only; large data files are downloaded at runtime and are not committed by default.
