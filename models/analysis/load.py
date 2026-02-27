"""Helpers for loading OHLC datasets for analysis."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from jobs.ohlc_config import REPO_ROOT, default_asset_config_path, load_ohlc_config


def load_ohlc_data(asset: str, config_path: str | Path | None = None) -> pd.DataFrame:
    """Load the configured OHLC parquet for an asset as a pandas DataFrame."""
    asset_id = asset.strip().lower()
    if not asset_id:
        raise ValueError("Asset id must be non-empty.")

    resolved_config_path = Path(config_path) if config_path else default_asset_config_path(asset_id)
    cfg = load_ohlc_config(resolved_config_path)
    pipeline_cfg = cfg.get("pipeline", {})

    data_dir = Path(str(pipeline_cfg.get("data_dir", "data")))
    if not data_dir.is_absolute():
        data_dir = REPO_ROOT / data_dir

    outputs = pipeline_cfg.get("outputs", {})
    ohlc_out = Path(str(outputs.get("ohlc_hourly", f"{asset_id}_ohlc_hourly.parquet")))
    ohlc_path = ohlc_out if ohlc_out.is_absolute() else (data_dir / ohlc_out)

    if not ohlc_path.exists():
        raise FileNotFoundError(f"OHLC parquet not found for asset '{asset_id}': {ohlc_path}")

    return pd.read_parquet(ohlc_path)
