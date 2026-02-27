"""Config loading for OHLC Dagster pipelines."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OHLC_DATA_CONFIG = Path(
    os.getenv("OHLC_DATA_CONFIG", str(REPO_ROOT / "configs" / "ETH" / "ohlc_data.yaml"))
)
DEFAULT_OHLC_MASTER_CONFIG = Path(
    os.getenv("OHLC_MASTER_CONFIG", str(REPO_ROOT / "configs" / "ohlc_master.yaml"))
)
DEFAULT_CHAINLINK_ETH_USD_FEED = "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"


def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        return os.path.expandvars(value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


def load_ohlc_config(path: str | Path | None = None) -> dict[str, Any]:
    config_path = Path(path) if path else DEFAULT_OHLC_DATA_CONFIG
    data = yaml.safe_load(config_path.read_text()) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Invalid config in {config_path}; expected top-level mapping")
    return _expand_env(data)


def load_ohlc_master_config(path: str | Path | None = None) -> dict[str, Any]:
    config_path = Path(path) if path else DEFAULT_OHLC_MASTER_CONFIG
    data = yaml.safe_load(config_path.read_text()) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Invalid master config in {config_path}; expected top-level mapping")
    return _expand_env(data)


def default_asset_config_path(asset: str) -> Path:
    asset_key = asset.strip().lower()
    if not asset_key:
        raise ValueError("Asset id must be non-empty.")
    return REPO_ROOT / "configs" / asset_key.upper() / "ohlc_data.yaml"


def _output_path(data_dir: Path, filename: str) -> str:
    p = Path(filename)
    return str(p if p.is_absolute() else (data_dir / p))


def build_run_config_from_ohlc_config(path: str | Path | None = None) -> dict[str, object]:
    cfg = load_ohlc_config(path)
    pipeline_cfg = cfg.get("pipeline", {})
    sources_cfg = cfg.get("sources", {})
    chainlink_cfg = sources_cfg.get("chainlink", {})

    start = str(pipeline_cfg.get("start", "2021-01-01"))
    data_dir = Path(str(pipeline_cfg.get("data_dir", "data")))
    if not data_dir.is_absolute():
        data_dir = REPO_ROOT / data_dir

    outputs = pipeline_cfg.get("outputs", {})
    chainlink_raw = _output_path(data_dir, str(outputs.get("chainlink_raw", "chainlink_rounds.parquet")))
    ohlc_hourly = _output_path(data_dir, str(outputs.get("ohlc_hourly", "ohlc_hourly.parquet")))

    rpc_url = str(chainlink_cfg.get("rpc_url", os.getenv("INFURA_HTTP", "")))
    if not rpc_url or "${" in rpc_url:
        raise ValueError(
            "Chainlink RPC URL is not configured. Set INFURA_HTTP in the Dagster code-location "
            "environment or provide sources.chainlink.rpc_url in ohlc_data.yaml."
        )

    return {
        "ops": {
            "chainlink_ohlc": {
                "config": {
                    "rpc_url": rpc_url,
                    "feed": str(chainlink_cfg.get("feed", DEFAULT_CHAINLINK_ETH_USD_FEED)),
                    "start": start,
                    "raw_out": chainlink_raw,
                    "hourly_out": ohlc_hourly,
                    "max_staleness_sec": int(chainlink_cfg.get("max_staleness_sec", 7200)),
                }
            }
        }
    }


def iter_enabled_assets(master_path: str | Path | None = None) -> list[dict[str, str]]:
    cfg = load_ohlc_master_config(master_path)
    assets = cfg.get("assets", [])
    if not isinstance(assets, list):
        raise ValueError("Master config 'assets' must be a list.")

    enabled_assets: list[dict[str, str]] = []
    for item in assets:
        if not isinstance(item, dict):
            continue
        if not bool(item.get("enabled", True)):
            continue
        asset = str(item.get("id", "")).strip().lower()
        if not asset:
            raise ValueError("Each enabled asset entry must include non-empty 'id'.")
        config_path_raw = item.get("config_path")
        if config_path_raw:
            config_path = Path(str(config_path_raw))
            if not config_path.is_absolute():
                config_path = REPO_ROOT / config_path
        else:
            config_path = default_asset_config_path(asset)
        enabled_assets.append({"id": asset, "config_path": str(config_path)})
    return enabled_assets
