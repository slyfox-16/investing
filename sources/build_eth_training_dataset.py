#!/usr/bin/env python3
"""Join ETH spot + implied-volatility features into a training dataset.

Example:
  python -m sources.build_eth_training_dataset \
    --spot data/ethusd_hourly.parquet \
    --iv-index data/eth_iv_index_hourly.parquet \
    --iv-surface data/eth_iv_surface_hourly.parquet \
    --out data/eth_training_hourly.parquet
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


FEATURE_WINDOWS = (1, 6, 24)
RV_WINDOWS = (6, 24)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build ETH model training dataset from spot + IV features.")
    parser.add_argument("--spot", default="data/ethusd_hourly.parquet", help="Path to Chainlink hourly OHLC parquet.")
    parser.add_argument("--iv-index", default="data/eth_iv_index_hourly.parquet", help="Path to Deribit IV index parquet.")
    parser.add_argument("--iv-surface", default="data/eth_iv_surface_hourly.parquet", help="Path to Deribit IV surface parquet.")
    parser.add_argument("--out", default="data/eth_training_hourly.parquet", help="Output training dataset path.")
    return parser.parse_args()


def read_table(path: str, ts_col: str = "ts_hour") -> pd.DataFrame:
    df = pd.read_parquet(path)
    if ts_col not in df.columns:
        raise SystemExit(f"Missing expected timestamp column '{ts_col}' in {path}")
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True)
    df = df.sort_values(ts_col).drop_duplicates(subset=[ts_col], keep="last").reset_index(drop=True)
    return df


def future_realized_vol(log_ret: pd.Series, horizon: int) -> pd.Series:
    # sqrt(sum_{i=1..h} r_{t+i}^2)
    vals = log_ret.to_numpy(dtype=float)
    out = np.full_like(vals, np.nan)
    n = len(vals)
    for i in range(n):
        j = i + 1
        k = i + 1 + horizon
        if k > n:
            continue
        window = vals[j:k]
        if np.isfinite(window).all():
            out[i] = math.sqrt(float(np.square(window).sum()))
    return pd.Series(out, index=log_ret.index)


def run_build_eth_training_dataset(spot_path: str, iv_index_path: str, iv_surface_path: str, out_path: str) -> dict[str, Any]:
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    spot = read_table(spot_path)
    iv_idx = read_table(iv_index_path)
    iv_surface = read_table(iv_surface_path)

    out = spot.merge(iv_idx, on="ts_hour", how="left", suffixes=("", "_ividx"))
    out = out.merge(iv_surface, on="ts_hour", how="left", suffixes=("", "_ivsurf"))

    out = out.sort_values("ts_hour").reset_index(drop=True)

    close = out["close"].astype(float)
    log_ret_1h: Any = np.log(close / close.shift(1))

    for h in FEATURE_WINDOWS:
        out[f"ret_{h}h"] = close.shift(-h) / close - 1.0

    for h in RV_WINDOWS:
        out[f"rv_{h}h"] = future_realized_vol(log_ret_1h, horizon=h)

    out["source"] = "chainlink+deribit"
    out["ingested_at"] = pd.Timestamp.now(tz="UTC")

    if out["ts_hour"].duplicated().any():
        raise SystemExit("Duplicate ts_hour detected in final training dataset.")

    if not out["ts_hour"].dt.tz:
        raise SystemExit("ts_hour is not timezone-aware UTC.")

    out.to_parquet(out_path, index=False)

    print(f"Wrote {len(out)} rows to {out_path}")
    print(f"Date range: {out['ts_hour'].min()} -> {out['ts_hour'].max()}")
    print("Created targets: ret_1h, ret_6h, ret_24h, rv_6h, rv_24h")
    return {
        "rows": int(len(out)),
        "out": out_path,
        "ts_min": str(out["ts_hour"].min()),
        "ts_max": str(out["ts_hour"].max()),
    }


def main() -> int:
    args = parse_args()
    run_build_eth_training_dataset(
        spot_path=args.spot,
        iv_index_path=args.iv_index,
        iv_surface_path=args.iv_surface,
        out_path=args.out,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
