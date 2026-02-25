"""ETH training Dagster job definition."""

from __future__ import annotations

from dagster import ConfigMapping, Field, job

from jobs.config import build_run_config_from_training_config
from jobs.ops.eth_sources import eth_iv_index, eth_iv_surface, ethusd_hourly


def _map_top_level_config(cfg: dict[str, object]) -> dict[str, object]:
    config_path = cfg.get("training_config_path")
    path = str(config_path) if config_path else None
    return build_run_config_from_training_config(path)


@job(
    name="build_eth_training",
    config=ConfigMapping(
        config_fn=_map_top_level_config,
        config_schema={
            "training_config_path": Field(
                str,
                is_required=False,
                description=(
                    "Optional path to training_data.yaml. "
                    "Defaults to TRAINING_DATA_CONFIG env var or configs/ETH/training_data.yaml."
                ),
            )
        },
    ),
)
def build_eth_training() -> None:
    spot_done = ethusd_hourly()
    iv_index_done = eth_iv_index(spot_done)
    eth_iv_surface(iv_index_done)
