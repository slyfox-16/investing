#!/usr/bin/env python3
"""Backfill Deribit ETH volatility index candles and build hourly features.

Example:
  python -m sources.deribit.eth_iv_index \
    --start 2021-01-01 \
    --raw-out data/deribit_eth_dvol_raw.parquet \
    --hourly-out data/eth_iv_index_hourly.parquet
"""

from __future__ import annotations

import argparse
import json
import math
import time
from datetime import timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd

DEFAULT_BASE_URL = "https://www.deribit.com"
DEFAULT_START = "2021-01-01"
API_ENDPOINT_VERSION = "deribit_api_v2"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Deribit ETH volatility index candles.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Deribit API base URL.")
    parser.add_argument("--currency", default="ETH", help="Asset symbol (default ETH).")
    parser.add_argument("--resolution", default="60", help="Candle resolution in minutes (default 60).")
    parser.add_argument("--start", default=DEFAULT_START, help="UTC start (YYYY-MM-DD or ISO timestamp).")
    parser.add_argument("--end", help="UTC end (YYYY-MM-DD or ISO timestamp). Defaults to now.")
    parser.add_argument("--chunk-hours", type=int, default=24 * 30, help="API request chunk size in hours.")
    parser.add_argument("--sleep-sec", type=float, default=0.15, help="Sleep between API calls.")
    parser.add_argument("--timeout-sec", type=int, default=20, help="HTTP timeout in seconds.")
    parser.add_argument("--raw-out", default="data/deribit_eth_dvol_raw.parquet", help="Raw candle parquet output path.")
    parser.add_argument("--hourly-out", default="data/eth_iv_index_hourly.parquet", help="Normalized hourly parquet output path.")
    return parser.parse_args()


def to_utc_ts_millis(value: str | None, fallback_now: bool = False) -> int:
    if value is None:
        if not fallback_now:
            raise ValueError("Missing datetime value")
        return int(pd.Timestamp.now(tz="UTC").timestamp() * 1000)
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return int(ts.timestamp() * 1000)


def http_get_json(base_url: str, endpoint: str, params: dict[str, Any], timeout_sec: int) -> dict[str, Any]:
    query = urlencode(params)
    url = f"{base_url.rstrip('/')}/api/v2/{endpoint}?{query}"
    with urlopen(url, timeout=timeout_sec) as resp:  # noqa: S310
        return json.loads(resp.read().decode("utf-8"))


def parse_iv_value(value: Any) -> float | None:
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v) or v <= 0:
        return None
    if v > 5.0 and v <= 500.0:
        v = v / 100.0
    return v if 0 < v < 5 else None


def normalize_result_rows(payload: Any) -> pd.DataFrame:
    # Deribit returns list-like candles; defensively parse common structures.
    if isinstance(payload, dict):
        if "data" in payload:
            payload = payload["data"]
        elif all(k in payload for k in ("ticks", "open", "high", "low", "close")):
            df = pd.DataFrame(payload)
            rename_map = {"ticks": "ts_ms"}
            return df.rename(columns=rename_map)

    if not isinstance(payload, list):
        return pd.DataFrame(columns=["ts_ms", "open", "high", "low", "close"])

    rows: list[dict[str, Any]] = []
    for item in payload:
        if isinstance(item, dict):
            ts_ms = item.get("timestamp") or item.get("ticks") or item.get("time")
            rows.append(
                {
                    "ts_ms": ts_ms,
                    "open": item.get("open"),
                    "high": item.get("high"),
                    "low": item.get("low"),
                    "close": item.get("close"),
                }
            )
            continue
        if isinstance(item, (list, tuple)) and len(item) >= 5:
            rows.append(
                {
                    "ts_ms": item[0],
                    "open": item[1],
                    "high": item[2],
                    "low": item[3],
                    "close": item[4],
                }
            )

    return pd.DataFrame(rows, columns=["ts_ms", "open", "high", "low", "close"])


def fetch_history(
    base_url: str,
    currency: str,
    resolution: str,
    start: str,
    end: str | None,
    chunk_hours: int,
    sleep_sec: float,
    timeout_sec: int,
) -> pd.DataFrame:
    start_ms = to_utc_ts_millis(start)
    end_ms = to_utc_ts_millis(end, fallback_now=True)
    chunk_ms = int(chunk_hours * 3600 * 1000)

    rows: list[pd.DataFrame] = []
    cursor = start_ms
    while cursor < end_ms:
        chunk_end = min(cursor + chunk_ms, end_ms)
        result = http_get_json(
            base_url=base_url,
            endpoint="public/get_volatility_index_data",
            params={
                "currency": currency,
                "start_timestamp": cursor,
                "end_timestamp": chunk_end,
                "resolution": resolution,
            },
            timeout_sec=timeout_sec,
        )

        if "result" not in result:
            raise RuntimeError(f"Unexpected API response: {result}")

        df_chunk = normalize_result_rows(result["result"])
        if not df_chunk.empty:
            rows.append(df_chunk)

        cursor = chunk_end
        time.sleep(sleep_sec)

    if not rows:
        return pd.DataFrame(columns=["ts_ms", "open", "high", "low", "close"])

    out = pd.concat(rows, ignore_index=True)
    out = out.dropna(subset=["ts_ms"]).drop_duplicates(subset=["ts_ms"], keep="last")
    return out.sort_values("ts_ms").reset_index(drop=True)


def build_outputs(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df_raw.empty:
        raw_cols = ["ts", "open", "high", "low", "close", "source", "ingested_at", "api_endpoint_version"]
        norm_cols = [
            "ts_hour",
            "iv_index_open",
            "iv_index_high",
            "iv_index_low",
            "iv_index_close",
            "source",
            "ingested_at",
            "api_endpoint_version",
            "quality_flag",
        ]
        return pd.DataFrame(columns=raw_cols), pd.DataFrame(columns=norm_cols)

    df = df_raw.copy()
    df["ts"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)

    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].map(parse_iv_value)

    df = df.dropna(subset=["ts", "open", "high", "low", "close"]).sort_values("ts")
    ingested_at = pd.Timestamp.now(tz="UTC")

    df_raw_out = df[["ts", "open", "high", "low", "close"]].copy()
    df_raw_out["source"] = "deribit_dvol"
    df_raw_out["ingested_at"] = ingested_at
    df_raw_out["api_endpoint_version"] = API_ENDPOINT_VERSION

    hourly = (
        df.set_index("ts")[["open", "high", "low", "close"]]
        .resample("1h")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last"})
    )
    hourly = hourly.rename(
        columns={
            "open": "iv_index_open",
            "high": "iv_index_high",
            "low": "iv_index_low",
            "close": "iv_index_close",
        }
    )
    hourly["quality_flag"] = "ok"
    hourly = hourly.reset_index(names="ts_hour")
    hourly["source"] = "deribit_dvol"
    hourly["ingested_at"] = ingested_at
    hourly["api_endpoint_version"] = API_ENDPOINT_VERSION

    cols = [
        "ts_hour",
        "iv_index_open",
        "iv_index_high",
        "iv_index_low",
        "iv_index_close",
        "source",
        "ingested_at",
        "api_endpoint_version",
        "quality_flag",
    ]
    return df_raw_out, hourly[cols]


def run_deribit_eth_iv_index(
    base_url: str = DEFAULT_BASE_URL,
    currency: str = "ETH",
    resolution: str = "60",
    start: str = DEFAULT_START,
    end: str | None = None,
    chunk_hours: int = 24 * 30,
    sleep_sec: float = 0.15,
    timeout_sec: int = 20,
    raw_out_path: str = "data/deribit_eth_dvol_raw.parquet",
    hourly_out_path: str = "data/eth_iv_index_hourly.parquet",
) -> dict[str, str | int | float]:
    raw_api = fetch_history(
        base_url=base_url,
        currency=currency,
        resolution=resolution,
        start=start,
        end=end,
        chunk_hours=chunk_hours,
        sleep_sec=sleep_sec,
        timeout_sec=timeout_sec,
    )
    raw_out, hourly_out = build_outputs(raw_api)

    if raw_out.empty:
        raise SystemExit("No IV index rows fetched from Deribit.")

    if hourly_out["ts_hour"].duplicated().any():
        raise SystemExit("Duplicate ts_hour detected in hourly IV index output.")

    Path(raw_out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(hourly_out_path).parent.mkdir(parents=True, exist_ok=True)
    raw_out.to_parquet(raw_out_path, index=False)
    hourly_out.to_parquet(hourly_out_path, index=False)

    print(f"Wrote {len(raw_out)} rows to {raw_out_path}")
    print(f"Wrote {len(hourly_out)} rows to {hourly_out_path}")
    latest = hourly_out.iloc[-1]
    print(f"Latest IV close: {latest['iv_index_close']:.6f} at {latest['ts_hour']}")
    return {
        "raw_rows": int(len(raw_out)),
        "hourly_rows": int(len(hourly_out)),
        "raw_out": raw_out_path,
        "hourly_out": hourly_out_path,
        "latest_iv_close": float(latest["iv_index_close"]),
    }


def main() -> int:
    args = parse_args()
    run_deribit_eth_iv_index(
        base_url=args.base_url,
        currency=args.currency,
        resolution=args.resolution,
        start=args.start,
        end=args.end,
        chunk_hours=args.chunk_hours,
        sleep_sec=args.sleep_sec,
        timeout_sec=args.timeout_sec,
        raw_out_path=args.raw_out,
        hourly_out_path=args.hourly_out,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
