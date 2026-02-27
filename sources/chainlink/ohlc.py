#!/usr/bin/env python3
"""Backfill Chainlink rounds and build hourly OHLC features for a configured feed.

Example:
  python -m sources.chainlink.ohlc \
    --rpc-url https://mainnet.infura.io/v3/<key> \
    --raw-out data/chainlink_rounds.parquet \
    --hourly-out data/ohlc_hourly.parquet
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
import pandas as pd
import requests
from web3 import Web3
from web3.contract import Contract
from web3.exceptions import ContractLogicError

# Chainlink ETH / USD mainnet proxy
DEFAULT_ETH_USD_FEED = "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"
ROUND_MASK = (1 << 64) - 1

PROXY_ABI = [
    {
        "inputs": [{"internalType": "uint80", "name": "_roundId", "type": "uint80"}],
        "name": "getRoundData",
        "outputs": [
            {"internalType": "uint80", "name": "roundId", "type": "uint80"},
            {"internalType": "int256", "name": "answer", "type": "int256"},
            {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
            {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"internalType": "uint80", "name": "roundId", "type": "uint80"},
            {"internalType": "int256", "name": "answer", "type": "int256"},
            {"internalType": "uint256", "name": "startedAt", "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt", "type": "uint256"},
            {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
]


@dataclass
class Round:
    round_id: int
    answer: int
    started_at: int
    updated_at: int
    answered_in_round: int


class FeedReader:
    def __init__(self, contract: Contract):
        self.contract = contract
        self._phase_last_round_cache: dict[int, int] = {}

    @staticmethod
    def _compose_round_id(phase_id: int, agg_round_id: int) -> int:
        return (phase_id << 64) | agg_round_id

    @staticmethod
    def _split_round_id(round_id: int) -> tuple[int, int]:
        return (round_id >> 64, round_id & ROUND_MASK)

    def _get_round(self, round_id: int) -> Optional[Round]:
        try:
            rid, answer, started_at, updated_at, answered_in_round = self.contract.functions.getRoundData(round_id).call()
        except (ContractLogicError, ValueError):
            return None

        if int(updated_at) <= 0:
            return None

        return Round(
            round_id=int(rid),
            answer=int(answer),
            started_at=int(started_at),
            updated_at=int(updated_at),
            answered_in_round=int(answered_in_round),
        )

    def _round_exists(self, phase_id: int, agg_round_id: int) -> bool:
        if agg_round_id <= 0:
            return False
        rid = self._compose_round_id(phase_id, agg_round_id)
        return self._get_round(rid) is not None

    def _find_phase_last_round(self, phase_id: int) -> int:
        cached = self._phase_last_round_cache.get(phase_id)
        if cached is not None:
            return cached

        if phase_id <= 0:
            return 0

        if not self._round_exists(phase_id, 1):
            self._phase_last_round_cache[phase_id] = 0
            return 0

        lo = 1
        hi = 1
        # Exponential probe to find first non-existent upper bound.
        while self._round_exists(phase_id, hi):
            lo = hi
            hi *= 2

        # Binary search for max existent round in [lo, hi).
        left = lo
        right = hi - 1
        best = lo

        while left <= right:
            mid = (left + right) // 2
            if self._round_exists(phase_id, mid):
                best = mid
                left = mid + 1
            else:
                right = mid - 1

        self._phase_last_round_cache[phase_id] = best
        return best

    def previous_round_id(self, round_id: int) -> Optional[int]:
        phase_id, agg_round_id = self._split_round_id(round_id)

        if agg_round_id > 1:
            return self._compose_round_id(phase_id, agg_round_id - 1)

        prev_phase = phase_id - 1
        while prev_phase > 0:
            prev_last = self._find_phase_last_round(prev_phase)
            if prev_last > 0:
                return self._compose_round_id(prev_phase, prev_last)
            prev_phase -= 1

        return None

    def backfill(self, latest_round_id: int, min_timestamp: Optional[int], max_rounds: Optional[int]) -> list[Round]:
        rows: list[Round] = []
        current = latest_round_id

        while current is not None:
            row = self._get_round(current)
            if row is not None:
                rows.append(row)
                if min_timestamp is not None and row.updated_at < min_timestamp:
                    break

            if max_rounds is not None and len(rows) >= max_rounds:
                break

            current = self.previous_round_id(current)

        return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill Chainlink rounds and build hourly OHLC bars for a configured feed.")
    parser.add_argument("--rpc-url", default=os.getenv("INFURA_HTTP"), help="Ethereum RPC URL (or set INFURA_HTTP).")
    parser.add_argument("--feed", default=DEFAULT_ETH_USD_FEED, help="Chainlink feed proxy address.")
    parser.add_argument("--start", help="UTC start date (YYYY-MM-DD). Older rows are not backfilled.")
    parser.add_argument("--max-rounds", type=int, help="Optional cap on number of rounds to fetch.")
    parser.add_argument("--max-staleness-sec", type=int, default=7200, help="Gap threshold for is_gap flag.")
    parser.add_argument("--raw-out", default="data/chainlink_rounds.parquet", help="Output path for raw rounds parquet.")
    parser.add_argument("--hourly-out", default="data/ohlc_hourly.parquet", help="Output path for hourly parquet.")
    return parser.parse_args()


def build_hourly(df_rounds: pd.DataFrame, max_staleness_sec: int) -> pd.DataFrame:
    if df_rounds.empty:
        return pd.DataFrame(
            columns=["ts_hour", "open", "high", "low", "close", "n_updates", "staleness_sec", "is_gap"]
        )

    df_sorted = df_rounds.sort_values("updated_at").copy()
    price_series = df_sorted.set_index("updated_at")["price"]

    ohlc = price_series.resample("1h").ohlc()
    updates = price_series.resample("1h").count().rename("n_updates")

    hourly_index = pd.date_range(
        start=ohlc.index.min().floor("h"), end=ohlc.index.max().floor("h"), freq="1h", tz="UTC"
    )

    hourly = pd.DataFrame(index=hourly_index)
    hourly = hourly.join(ohlc).join(updates)
    hourly["n_updates"] = hourly["n_updates"].fillna(0).astype(int)

    # Forward-fill close, then use close for empty-hour OHLC so bars remain model-friendly.
    hourly["close"] = hourly["close"].ffill()
    for col in ["open", "high", "low"]:
        hourly[col] = hourly[col].fillna(hourly["close"])

    # Staleness measured at the end of each hour.
    updates_df = pd.DataFrame({"updated_at": df_sorted["updated_at"]}).drop_duplicates().sort_values("updated_at")
    hour_marks = pd.DataFrame({"ts_hour": hourly.index})
    hour_marks["hour_end"] = hour_marks["ts_hour"] + pd.Timedelta(hours=1)

    asof = pd.merge_asof(
        hour_marks.sort_values("hour_end"),
        updates_df,
        left_on="hour_end",
        right_on="updated_at",
        direction="backward",
    )

    staleness = (asof["hour_end"] - asof["updated_at"]).dt.total_seconds()
    hourly["staleness_sec"] = staleness.values
    hourly["is_gap"] = hourly["staleness_sec"] > max_staleness_sec

    out = hourly.reset_index(names="ts_hour")
    return out[["ts_hour", "open", "high", "low", "close", "n_updates", "staleness_sec", "is_gap"]]


def run_chainlink_ohlc(
    rpc_url: str,
    feed: str = DEFAULT_ETH_USD_FEED,
    start: str | None = None,
    max_rounds: int | None = None,
    max_staleness_sec: int = 7200,
    raw_out: str = "data/chainlink_rounds.parquet",
    hourly_out: str = "data/ohlc_hourly.parquet",
) -> dict[str, str | int | float]:
    if not rpc_url:
        raise RuntimeError("Missing RPC URL. Pass --rpc-url or set INFURA_HTTP.")

    # Probe endpoint first so auth/config errors are explicit in Dagster logs.
    try:
        probe = requests.post(
            rpc_url,
            json={"jsonrpc": "2.0", "method": "web3_clientVersion", "params": [], "id": 1},
            timeout=15,
        )
        body = probe.text[:240]
        if probe.status_code == 401 and "invalid project id" in body.lower():
            raise RuntimeError(
                "INFURA_HTTP is configured, but Infura returned 'invalid project id'. "
                "Use a valid active Infura Mainnet endpoint URL."
            )
        if probe.status_code >= 400:
            raise RuntimeError(f"RPC endpoint returned HTTP {probe.status_code}: {body}")
    except requests.RequestException as exc:
        raise RuntimeError(f"RPC endpoint request failed: {exc}") from exc

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        raise RuntimeError("Failed to connect to RPC. Verify INFURA_HTTP and network access from the code server.")

    feed_addr = Web3.to_checksum_address(feed)
    feed = w3.eth.contract(address=feed_addr, abi=PROXY_ABI)

    try:
        decimals = int(feed.functions.decimals().call())
        latest = feed.functions.latestRoundData().call()
    except Exception as exc:
        raise RuntimeError(f"Failed reading feed contract: {exc}") from exc

    latest_round_id = int(latest[0])
    min_timestamp = None
    if start:
        min_timestamp = int(pd.Timestamp(start, tz="UTC").timestamp())

    reader = FeedReader(feed)
    rounds = reader.backfill(latest_round_id=latest_round_id, min_timestamp=min_timestamp, max_rounds=max_rounds)

    if not rounds:
        raise RuntimeError("No Chainlink rounds fetched for the configured start/range.")

    df = pd.DataFrame([r.__dict__ for r in rounds])
    df = df.sort_values("updated_at").drop_duplicates(subset=["round_id"], keep="last")
    df["updated_at"] = pd.to_datetime(df["updated_at"], unit="s", utc=True)
    df["started_at"] = pd.to_datetime(df["started_at"], unit="s", utc=True)
    df["price"] = df["answer"] / (10 ** decimals)

    raw_cols = ["round_id", "answer", "price", "started_at", "updated_at", "answered_in_round"]
    df_raw = df[raw_cols].copy()
    # round_id / answered_in_round are uint80 values and can exceed int64 in later feed phases.
    # Persist them as strings in the raw dump so parquet serialization is stable across engines.
    df_raw["round_id"] = df_raw["round_id"].map(str)
    df_raw["answered_in_round"] = df_raw["answered_in_round"].map(str)

    hourly = build_hourly(df_raw, max_staleness_sec=max_staleness_sec)

    Path(raw_out).parent.mkdir(parents=True, exist_ok=True)
    Path(hourly_out).parent.mkdir(parents=True, exist_ok=True)
    df_raw.to_parquet(raw_out, index=False)
    hourly.to_parquet(hourly_out, index=False)

    print(f"Wrote {len(df_raw)} raw rounds to {raw_out}")
    print(f"Wrote {len(hourly)} hourly rows to {hourly_out}")
    print(f"Latest hourly close: {hourly.iloc[-1]['close']:.6f} at {hourly.iloc[-1]['ts_hour']}")
    return {
        "raw_rows": int(len(df_raw)),
        "hourly_rows": int(len(hourly)),
        "raw_out": raw_out,
        "hourly_out": hourly_out,
        "latest_close": float(hourly.iloc[-1]["close"]),
    }


def main() -> int:
    load_dotenv()
    args = parse_args()
    run_chainlink_ohlc(
        rpc_url=args.rpc_url,
        feed=args.feed,
        start=args.start,
        max_rounds=args.max_rounds,
        max_staleness_sec=args.max_staleness_sec,
        raw_out=args.raw_out,
        hourly_out=args.hourly_out,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
