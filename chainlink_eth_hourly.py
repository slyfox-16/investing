#!/usr/bin/env python3
"""Backfill Chainlink ETH/USD rounds and build hourly OHLC features.

Example:
  python chainlink_eth_hourly.py \
    --rpc-url https://eth-mainnet.g.alchemy.com/v2/<key> \
    --raw-out chainlink_rounds.parquet \
    --hourly-out ethusd_hourly.parquet
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Optional

import pandas as pd
from web3 import Web3
from web3.contract import Contract
from web3.exceptions import ContractLogicError

# Chainlink ETH / USD mainnet proxy
DEFAULT_FEED = "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"
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
    parser = argparse.ArgumentParser(description="Backfill Chainlink rounds and build hourly ETH/USD bars.")
    parser.add_argument("--rpc-url", default=os.getenv("RPC_URL"), help="Ethereum RPC URL (or set RPC_URL).")
    parser.add_argument("--feed", default=DEFAULT_FEED, help="Chainlink feed proxy address.")
    parser.add_argument("--start", help="UTC start date (YYYY-MM-DD). Older rows are not backfilled.")
    parser.add_argument("--max-rounds", type=int, help="Optional cap on number of rounds to fetch.")
    parser.add_argument("--max-staleness-sec", type=int, default=7200, help="Gap threshold for is_gap flag.")
    parser.add_argument("--raw-out", default="chainlink_rounds.parquet", help="Output path for raw rounds parquet.")
    parser.add_argument("--hourly-out", default="ethusd_hourly.parquet", help="Output path for hourly parquet.")
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


def main() -> int:
    args = parse_args()

    if not args.rpc_url:
        print("Missing RPC URL. Pass --rpc-url or set RPC_URL.", file=sys.stderr)
        return 2

    w3 = Web3(Web3.HTTPProvider(args.rpc_url))
    if not w3.is_connected():
        print("Failed to connect to RPC.", file=sys.stderr)
        return 2

    feed_addr = Web3.to_checksum_address(args.feed)
    feed = w3.eth.contract(address=feed_addr, abi=PROXY_ABI)

    try:
        decimals = int(feed.functions.decimals().call())
        latest = feed.functions.latestRoundData().call()
    except Exception as exc:
        print(f"Failed reading feed contract: {exc}", file=sys.stderr)
        return 2

    latest_round_id = int(latest[0])
    min_timestamp = None
    if args.start:
        min_timestamp = int(pd.Timestamp(args.start, tz="UTC").timestamp())

    reader = FeedReader(feed)
    rounds = reader.backfill(latest_round_id=latest_round_id, min_timestamp=min_timestamp, max_rounds=args.max_rounds)

    if not rounds:
        print("No rounds fetched.", file=sys.stderr)
        return 1

    df = pd.DataFrame([r.__dict__ for r in rounds])
    df = df.sort_values("updated_at").drop_duplicates(subset=["round_id"], keep="last")
    df["updated_at"] = pd.to_datetime(df["updated_at"], unit="s", utc=True)
    df["started_at"] = pd.to_datetime(df["started_at"], unit="s", utc=True)
    df["price"] = df["answer"] / (10 ** decimals)

    raw_cols = ["round_id", "answer", "price", "started_at", "updated_at", "answered_in_round"]
    df_raw = df[raw_cols].copy()

    hourly = build_hourly(df_raw, max_staleness_sec=args.max_staleness_sec)

    df_raw.to_parquet(args.raw_out, index=False)
    hourly.to_parquet(args.hourly_out, index=False)

    print(f"Wrote {len(df_raw)} raw rounds to {args.raw_out}")
    print(f"Wrote {len(hourly)} hourly rows to {args.hourly_out}")
    print(f"Latest hourly close: {hourly.iloc[-1]['close']:.6f} at {hourly.iloc[-1]['ts_hour']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
