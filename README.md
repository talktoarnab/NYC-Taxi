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
| `app.py` | Streamlit dashboard over Gold + KPI files |
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

On success you get row-count audits in the console, a Gold Parquet file, and KPI files under `output/`.

## Run the web app (Streamlit)

```bash
streamlit run app.py
```

The UI shows a sample of the Gold table, key metrics, and four KPI tabs. You can use **Run full ETL pipeline** in the app to build data from scratch, or run `python -m nyc_taxi` first and refresh.

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

- **Trips** — e.g. `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet` (configurable in `nyc_taxi.config.Config.parquet_url`).
- **Zones** — `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`

To use another month or year, point `parquet_url` and the `parquet_path` property pattern at the matching TLC filename (or download manually into `data/raw/` and adjust `Config`).

## Deployment (Streamlit, GitHub Actions, OCI, AWS)

Step-by-step guides (Streamlit Community Cloud with ETL in GitHub Actions, Oracle Cloud VM, and AWS) are in **[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)**. The repository includes a sample **[`.github/workflows/etl.yml`](.github/workflows/etl.yml)** and a root **[`Dockerfile`](Dockerfile)** for container-based hosts.

## License and data terms

Trip data and zone shapes are **public NYC TLC** material; follow [NYC’s open data / TLC terms of use](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) for redistribution and attribution. This repository contains code only; large data files are downloaded at runtime and are not committed by default.
