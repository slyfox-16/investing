"""Dagster repository entrypoint.

Dagster should load `defs` from this module.
"""

from dagster import Definitions

from jobs.ohlc_config import build_run_config_from_ohlc_config
from jobs.pipelines import build_all_ohlc_data, build_eth_ohlc_data


defs = Definitions(jobs=[build_eth_ohlc_data, build_all_ohlc_data])

__all__ = ["defs", "build_eth_ohlc_data", "build_all_ohlc_data", "build_run_config_from_ohlc_config"]
