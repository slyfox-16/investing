"""Config loading for Dagster ETH training pipeline."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = Path(
    os.getenv("TRAINING_DATA_CONFIG", str(REPO_ROOT / "configs" / "ETH" / "training_data.yaml"))
)
DEFAULT_CHAINLINK_FEED = "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"
DEFAULT_DERIBIT_BASE_URL = "https://www.deribit.com"


def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        return os.path.expandvars(value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


def load_training_config(path: str | Path | None = None) -> dict[str, Any]:
    config_path = Path(path) if path else DEFAULT_CONFIG_PATH
    data = yaml.safe_load(config_path.read_text()) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Invalid config in {config_path}; expected top-level mapping")
    return _expand_env(data)


def _output_path(data_dir: Path, filename: str) -> str:
    p = Path(filename)
    return str(p if p.is_absolute() else (data_dir / p))


def build_run_config_from_training_config(path: str | Path | None = None) -> dict[str, object]:
    cfg = load_training_config(path)

    pipeline_cfg = cfg.get("pipeline", {})
    sources_cfg = cfg.get("sources", {})

    start = str(pipeline_cfg.get("start", "2021-01-01"))
    data_dir = Path(str(pipeline_cfg.get("data_dir", "data")))
    if not data_dir.is_absolute():
        data_dir = REPO_ROOT / data_dir

    outputs = pipeline_cfg.get("outputs", {})
    chainlink = sources_cfg.get("chainlink", {})
    deribit = sources_cfg.get("deribit", {})
    deribit_iv_index = deribit.get("iv_index", {})
    deribit_surface = deribit.get("iv_surface", {})

    chainlink_raw = _output_path(data_dir, str(outputs.get("chainlink_raw", "chainlink_rounds.parquet")))
    spot_hourly = _output_path(data_dir, str(outputs.get("spot_hourly", "ethusd_hourly.parquet")))
    iv_index_raw = _output_path(data_dir, str(outputs.get("deribit_iv_index_raw", "deribit_eth_dvol_raw.parquet")))
    iv_index_hourly = _output_path(data_dir, str(outputs.get("deribit_iv_index_hourly", "eth_iv_index_hourly.parquet")))
    iv_surface_raw = _output_path(data_dir, str(outputs.get("deribit_iv_surface_raw", "deribit_eth_option_surface_raw.parquet")))
    iv_surface_hourly = _output_path(data_dir, str(outputs.get("deribit_iv_surface_hourly", "eth_iv_surface_hourly.parquet")))
    training_out = _output_path(data_dir, str(outputs.get("training", "eth_training_hourly.parquet")))

    rpc_url = str(chainlink.get("rpc_url", os.getenv("INFURA_HTTP", "")))

    return {
        "ops": {
            "ethusd_hourly": {
                "config": {
                    "rpc_url": rpc_url,
                    "feed": str(chainlink.get("feed", DEFAULT_CHAINLINK_FEED)),
                    "start": start,
                    "raw_out": chainlink_raw,
                    "hourly_out": spot_hourly,
                    "max_staleness_sec": int(chainlink.get("max_staleness_sec", 7200)),
                }
            },
            "eth_IV_index": {
                "config": {
                    "base_url": str(deribit.get("base_url", DEFAULT_DERIBIT_BASE_URL)),
                    "currency": str(deribit.get("currency", "ETH")),
                    "resolution": str(deribit_iv_index.get("resolution", "60")),
                    "start": str(deribit_iv_index.get("start", start)),
                    "chunk_hours": int(deribit_iv_index.get("chunk_hours", 24 * 30)),
                    "sleep_sec": float(deribit_iv_index.get("sleep_sec", 0.15)),
                    "timeout_sec": int(deribit_iv_index.get("timeout_sec", 20)),
                    "raw_out": iv_index_raw,
                    "hourly_out": iv_index_hourly,
                }
            },
            "eth_iv_surface": {
                "config": {
                    "base_url": str(deribit.get("base_url", DEFAULT_DERIBIT_BASE_URL)),
                    "currency": str(deribit.get("currency", "ETH")),
                    "timeout_sec": int(deribit_surface.get("timeout_sec", 20)),
                    "surface_raw_out": iv_surface_raw,
                    "surface_hourly_out": iv_surface_hourly,
                    "training_out": training_out,
                    "spot_hourly_in": spot_hourly,
                    "iv_index_hourly_in": iv_index_hourly,
                    "min_contracts": int(deribit_surface.get("min_contracts", 25)),
                    "min_total_oi": float(deribit_surface.get("min_total_oi", 50.0)),
                    "max_staleness_hours": int(deribit_surface.get("max_staleness_hours", 24)),
                }
            },
        }
    }
