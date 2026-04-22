"""
NYC Taxi & Limousine Commission (TLC) Yellow Taxi ETL.

Public API: `run_pipeline`, `Config`, `PipelineResult`.
Command line: `python -m nyc_taxi`. Web UI: `streamlit run app.py` (see `app.py`).
"""
from nyc_taxi.config import Config, default_config
from nyc_taxi.pipeline import PipelineResult, run_pipeline

__all__ = [
    "Config",
    "PipelineResult",
    "default_config",
    "run_pipeline",
]
