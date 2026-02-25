"""Dagster repository entrypoint.

Dagster should load `defs` from this module.
"""

from dagster import Definitions

from jobs.config import build_run_config_from_training_config
from jobs.pipelines import build_eth_training


defs = Definitions(jobs=[build_eth_training])

__all__ = ["defs", "build_eth_training", "build_run_config_from_training_config"]
