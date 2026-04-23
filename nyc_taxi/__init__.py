"""
NYC Taxi & Limousine Commission (TLC) Yellow Taxi ETL.

Public API: `run_pipeline`, `Config`, `PipelineResult`.
Command line: `python -m nyc_taxi`. Web UI: `streamlit run app.py` (see `app.py`).
"""
from nyc_taxi.config import (
    Config,
    ParquetUrlEnvReport,
    default_config,
    report_parquet_url_from_environ,
)

__all__ = [
    "Config",
    "ParquetUrlEnvReport",
    "PipelineResult",
    "default_config",
    "report_parquet_url_from_environ",
    "run_pipeline",
]


def __getattr__(name: str):
    if name == "run_pipeline":
        from nyc_taxi.pipeline import run_pipeline as rp

        return rp
    if name == "PipelineResult":
        from nyc_taxi.pipeline import PipelineResult as Pr

        return Pr
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
