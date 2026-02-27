"""Master OHLC Dagster job definition for multiple assets."""

from __future__ import annotations

from dagster import ConfigMapping, Field, job

from jobs.ops.ohlc_sources import run_all_ohlc_assets
from jobs.ohlc_config import build_run_config_from_ohlc_config
from jobs.ops.ohlc_sources import chainlink_ohlc


def _map_top_level_config(cfg: dict[str, object]) -> dict[str, object]:
    config_path = cfg.get("ohlc_config_path")
    path = str(config_path) if config_path else None
    return build_run_config_from_ohlc_config(path)


@job(
    name="build_eth_ohlc_data",
    config=ConfigMapping(
        config_fn=_map_top_level_config,
        config_schema={
            "ohlc_config_path": Field(
                str,
                is_required=False,
                description=(
                    "Optional path to ohlc_data.yaml. "
                    "Defaults to OHLC_DATA_CONFIG env var or configs/ETH/ohlc_data.yaml."
                ),
            )
        },
    ),
)
def build_eth_ohlc_data() -> None:
    chainlink_ohlc()

def _map_master_config(cfg: dict[str, object]) -> dict[str, object]:
    master_config_path = str(cfg.get("master_config_path", "") or "")
    return {"ops": {"run_all_ohlc_assets": {"config": {"master_config_path": master_config_path}}}}


@job(
    name="build_all_ohlc_data",
    config=ConfigMapping(
        config_fn=_map_master_config,
        config_schema={
            "master_config_path": Field(
                str,
                is_required=False,
                description=(
                    "Optional path to ohlc_master.yaml. "
                    "Defaults to OHLC_MASTER_CONFIG env var or configs/ohlc_master.yaml."
                ),
            )
        },
    ),
)
def build_all_ohlc_data() -> None:
    run_all_ohlc_assets()
