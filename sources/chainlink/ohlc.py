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
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import requests
from web3 import Web3
from web3.contract import Contract
from web3.exceptions import ContractLogicError

# Chainlink ETH / USD mainnet proxy
DEFAULT_ETH_USD_FEED = "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"
ROUND_MASK = (1 << 64) - 1
RAW_COLS = ["round_id", "answer", "price", "started_at", "updated_at", "answered_in_round"]
HOURLY_COLS = ["ts_hour", "open", "high", "low", "close", "n_updates", "staleness_sec", "is_gap"]

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
    parser.add_argument(
        "--update-overlap-hours",
        type=int,
        default=6,
        help="Hours of overlap to re-pull when updating existing hourly output.",
    )
    parser.add_argument("--raw-out", default="data/chainlink_rounds.parquet", help="Output path for raw rounds parquet.")
    parser.add_argument("--hourly-out", default="data/ohlc_hourly.parquet", help="Output path for hourly parquet (parquet backend).")
    parser.add_argument(
        "--storage-backend",
        default="parquet",
        choices=["parquet", "postgres"],
        help="Storage backend for hourly OHLC output.",
    )
    parser.add_argument(
        "--pg-env-prefix",
        default="INVESTING_PG",
        help="Env prefix for Postgres credentials: <PREFIX>_HOST/_PORT/_DB/_USER/_PASSWORD.",
    )
    parser.add_argument("--pg-schema", default="public", help="Target Postgres schema for hourly table.")
    parser.add_argument("--pg-table", default="eth_ohlc_hourly", help="Target Postgres table for hourly rows.")
    return parser.parse_args()


def build_hourly(df_rounds: pd.DataFrame, max_staleness_sec: int) -> pd.DataFrame:
    if df_rounds.empty:
        return pd.DataFrame(columns=HOURLY_COLS)

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
    return out[HOURLY_COLS]


def _parse_utc_timestamp(value: str) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _normalize_raw(df: pd.DataFrame) -> pd.DataFrame:
    missing = set(RAW_COLS) - set(df.columns)
    if missing:
        missing_fmt = ", ".join(sorted(missing))
        raise RuntimeError(f"Raw parquet is missing expected columns: {missing_fmt}")

    out = df[RAW_COLS].copy()
    out["round_id"] = out["round_id"].astype(str)
    out["answered_in_round"] = out["answered_in_round"].astype(str)
    out["started_at"] = pd.to_datetime(out["started_at"], utc=True, errors="coerce")
    out["updated_at"] = pd.to_datetime(out["updated_at"], utc=True, errors="coerce")
    out = out.dropna(subset=["updated_at"])
    return out


def _normalize_hourly(df: pd.DataFrame) -> pd.DataFrame:
    missing = set(HOURLY_COLS) - set(df.columns)
    if missing:
        missing_fmt = ", ".join(sorted(missing))
        raise RuntimeError(f"Hourly data is missing expected columns: {missing_fmt}")

    out = df[HOURLY_COLS].copy()
    out["ts_hour"] = pd.to_datetime(out["ts_hour"], utc=True, errors="coerce")
    out = out.dropna(subset=["ts_hour"])
    out["n_updates"] = out["n_updates"].fillna(0).astype(int)
    out["is_gap"] = out["is_gap"].astype(bool)
    return out


def _write_parquet_atomic(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        df.to_parquet(tmp_path, index=False)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _load_postgres_credentials(pg_env_prefix: str) -> dict[str, str | int]:
    prefix = str(pg_env_prefix).strip().upper()
    if not prefix:
        raise RuntimeError("pg_env_prefix must be non-empty for postgres storage backend.")

    host = os.getenv(f"{prefix}_HOST", "").strip()
    port_raw = os.getenv(f"{prefix}_PORT", "5432").strip()
    dbname = os.getenv(f"{prefix}_DB", "").strip()
    user = os.getenv(f"{prefix}_USER", "").strip()
    password = os.getenv(f"{prefix}_PASSWORD", "").strip()

    missing = [
        key
        for key, value in [
            (f"{prefix}_HOST", host),
            (f"{prefix}_PORT", port_raw),
            (f"{prefix}_DB", dbname),
            (f"{prefix}_USER", user),
            (f"{prefix}_PASSWORD", password),
        ]
        if not value
    ]
    if missing:
        missing_fmt = ", ".join(missing)
        raise RuntimeError(f"Missing required Postgres env vars: {missing_fmt}")

    try:
        port = int(port_raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid integer for {prefix}_PORT: {port_raw}") from exc

    return {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password,
    }


def _connect_postgres(creds: dict[str, str | int]):
    return psycopg2.connect(
        host=str(creds["host"]),
        port=int(creds["port"]),
        dbname=str(creds["dbname"]),
        user=str(creds["user"]),
        password=str(creds["password"]),
    )


def _ensure_hourly_table(conn, schema: str, table: str) -> None:
    schema_name = schema.strip() or "public"
    table_name = table.strip() or "eth_ohlc_hourly"
    index_name = f"{table_name}_ts_hour_desc_idx"

    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    ts_hour timestamptz PRIMARY KEY,
                    open double precision NOT NULL,
                    high double precision NOT NULL,
                    low double precision NOT NULL,
                    close double precision NOT NULL,
                    n_updates bigint NOT NULL,
                    staleness_sec double precision,
                    is_gap boolean NOT NULL,
                    ingested_at timestamptz NOT NULL DEFAULT NOW(),
                    updated_at_ingest timestamptz NOT NULL DEFAULT NOW()
                )
                """
            ).format(sql.Identifier(schema_name), sql.Identifier(table_name))
        )
        cur.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {}.{} (ts_hour DESC)").format(
                sql.Identifier(index_name),
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
        )
    conn.commit()


def _to_utc_ts(value) -> pd.Timestamp | None:
    if value is None:
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _postgres_hourly_stats(conn, schema: str, table: str) -> tuple[int, pd.Timestamp | None]:
    schema_name = schema.strip() or "public"
    table_name = table.strip() or "eth_ohlc_hourly"

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT COUNT(*), MAX(ts_hour) FROM {}.{}").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
        )
        row = cur.fetchone()

    count = int(row[0]) if row and row[0] is not None else 0
    latest_ts = _to_utc_ts(row[1] if row else None)
    return count, latest_ts


def _postgres_upsert_hourly(conn, schema: str, table: str, hourly_df: pd.DataFrame) -> int:
    if hourly_df.empty:
        return 0

    schema_name = schema.strip() or "public"
    table_name = table.strip() or "eth_ohlc_hourly"

    rows: list[tuple[object, ...]] = []
    for row in hourly_df.itertuples(index=False):
        ts_hour = _to_utc_ts(row.ts_hour)
        if ts_hour is None:
            continue
        staleness = None if pd.isna(row.staleness_sec) else float(row.staleness_sec)
        rows.append(
            (
                ts_hour.to_pydatetime(),
                float(row.open),
                float(row.high),
                float(row.low),
                float(row.close),
                int(row.n_updates),
                staleness,
                bool(row.is_gap),
            )
        )

    if not rows:
        return 0

    query = sql.SQL(
        """
        INSERT INTO {}.{} (
            ts_hour, open, high, low, close, n_updates, staleness_sec, is_gap
        ) VALUES %s
        ON CONFLICT (ts_hour) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            n_updates = EXCLUDED.n_updates,
            staleness_sec = EXCLUDED.staleness_sec,
            is_gap = EXCLUDED.is_gap,
            updated_at_ingest = NOW()
        """
    ).format(sql.Identifier(schema_name), sql.Identifier(table_name))

    with conn.cursor() as cur:
        execute_values(cur, query.as_string(conn), rows, page_size=1000)
    conn.commit()
    return len(rows)


def _read_existing_hourly_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=HOURLY_COLS)
    return _normalize_hourly(pd.read_parquet(path))


def run_chainlink_ohlc(
    rpc_url: str,
    feed: str = DEFAULT_ETH_USD_FEED,
    start: str | None = None,
    max_rounds: int | None = None,
    max_staleness_sec: int = 7200,
    raw_out: str = "data/chainlink_rounds.parquet",
    hourly_out: str = "data/ohlc_hourly.parquet",
    update_overlap_hours: int = 6,
    storage_backend: str = "parquet",
    pg_env_prefix: str = "INVESTING_PG",
    pg_schema: str = "public",
    pg_table: str = "eth_ohlc_hourly",
) -> dict[str, object]:
    if not rpc_url:
        raise RuntimeError("Missing RPC URL. Pass --rpc-url or set INFURA_HTTP.")
    if update_overlap_hours < 0:
        raise ValueError("update_overlap_hours must be >= 0.")

    storage_backend = str(storage_backend).strip().lower()
    if storage_backend not in {"parquet", "postgres"}:
        raise ValueError("storage_backend must be one of: parquet, postgres")

    raw_path = Path(raw_out)
    hourly_path = Path(hourly_out)

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
    feed_contract = w3.eth.contract(address=feed_addr, abi=PROXY_ABI)

    try:
        decimals = int(feed_contract.functions.decimals().call())
        latest = feed_contract.functions.latestRoundData().call()
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Failed reading feed contract: {exc}") from exc

    config_start_ts = _parse_utc_timestamp(start) if start else None

    hourly_existing_rows = 0
    existing_hourly_max_ts: pd.Timestamp | None = None
    if storage_backend == "postgres":
        creds = _load_postgres_credentials(pg_env_prefix)
        with _connect_postgres(creds) as conn:
            _ensure_hourly_table(conn, pg_schema, pg_table)
            hourly_existing_rows, existing_hourly_max_ts = _postgres_hourly_stats(conn, pg_schema, pg_table)
    else:
        existing_hourly = _read_existing_hourly_parquet(hourly_path)
        hourly_existing_rows = int(len(existing_hourly))
        if not existing_hourly.empty:
            existing_hourly_max_ts = existing_hourly["ts_hour"].max()

    mode = "bootstrap"
    effective_start_ts = config_start_ts
    if existing_hourly_max_ts is not None:
        mode = "incremental"
        overlap_start_ts = existing_hourly_max_ts - pd.Timedelta(hours=update_overlap_hours)
        if config_start_ts is None:
            effective_start_ts = overlap_start_ts
        else:
            effective_start_ts = max(config_start_ts, overlap_start_ts)

    min_timestamp = int(effective_start_ts.timestamp()) if effective_start_ts is not None else None

    reader = FeedReader(feed_contract)
    latest_round_id = int(latest[0])
    rounds = reader.backfill(latest_round_id=latest_round_id, min_timestamp=min_timestamp, max_rounds=max_rounds)

    if not rounds:
        raise RuntimeError("No Chainlink rounds fetched for the configured start/range.")

    df = pd.DataFrame([r.__dict__ for r in rounds])
    df = df.sort_values("updated_at").drop_duplicates(subset=["round_id"], keep="last")
    df["updated_at"] = pd.to_datetime(df["updated_at"], unit="s", utc=True)
    df["started_at"] = pd.to_datetime(df["started_at"], unit="s", utc=True)
    df["price"] = df["answer"] / (10 ** decimals)

    df_raw = df[RAW_COLS].copy()
    # round_id / answered_in_round are uint80 values and can exceed int64 in later feed phases.
    # Persist them as strings in the raw dump so parquet serialization is stable across engines.
    df_raw["round_id"] = df_raw["round_id"].map(str)
    df_raw["answered_in_round"] = df_raw["answered_in_round"].map(str)
    df_raw = _normalize_raw(df_raw)

    new_hourly = _normalize_hourly(build_hourly(df_raw, max_staleness_sec=max_staleness_sec))

    existing_raw = pd.DataFrame(columns=RAW_COLS)
    if raw_path.exists():
        existing_raw = _normalize_raw(pd.read_parquet(raw_path))

    raw_existing_rows = int(len(existing_raw))
    raw_new_rows = int(len(df_raw))
    hourly_new_rows = int(len(new_hourly))

    merged_raw = pd.concat([existing_raw, df_raw], ignore_index=True)
    merged_raw = merged_raw.drop_duplicates(subset=["round_id"], keep="last")
    merged_raw = merged_raw.sort_values("updated_at", kind="mergesort").reset_index(drop=True)
    _write_parquet_atomic(merged_raw, raw_path)

    hourly_upserted_rows = 0
    hourly_rows = 0
    db_existing_latest_ts_iso = existing_hourly_max_ts.isoformat() if existing_hourly_max_ts is not None else ""
    db_latest_ts_iso = ""

    if storage_backend == "postgres":
        creds = _load_postgres_credentials(pg_env_prefix)
        with _connect_postgres(creds) as conn:
            _ensure_hourly_table(conn, pg_schema, pg_table)
            hourly_upserted_rows = _postgres_upsert_hourly(conn, pg_schema, pg_table, new_hourly)
            hourly_rows, db_latest_ts = _postgres_hourly_stats(conn, pg_schema, pg_table)
            db_latest_ts_iso = db_latest_ts.isoformat() if db_latest_ts is not None else ""
        latest_row = new_hourly.sort_values("ts_hour").iloc[-1]
        print(
            "Hourly storage=postgres "
            f"table={pg_schema}.{pg_table} existing={hourly_existing_rows} "
            f"new={hourly_new_rows} upserted={hourly_upserted_rows} total={hourly_rows}"
        )
    else:
        existing_hourly = _read_existing_hourly_parquet(hourly_path)
        merged_hourly = pd.concat([existing_hourly, new_hourly], ignore_index=True)
        merged_hourly = merged_hourly.drop_duplicates(subset=["ts_hour"], keep="last")
        merged_hourly = merged_hourly.sort_values("ts_hour", kind="mergesort").reset_index(drop=True)
        _write_parquet_atomic(merged_hourly, hourly_path)
        hourly_rows = int(len(merged_hourly))
        hourly_upserted_rows = hourly_new_rows
        latest_row = merged_hourly.iloc[-1]
        db_latest_ts_iso = ""

    effective_start_iso = effective_start_ts.isoformat() if effective_start_ts is not None else ""
    print(
        f"Mode={mode}, storage_backend={storage_backend}, effective_start={effective_start_iso}, "
        f"overlap_hours={update_overlap_hours}"
    )
    print(f"Wrote raw rows to {raw_out}: existing={raw_existing_rows} new={raw_new_rows} total={len(merged_raw)}")
    print(
        f"Hourly rows: existing={hourly_existing_rows} new={hourly_new_rows} "
        f"upserted={hourly_upserted_rows} total={hourly_rows}"
    )
    print(f"Latest hourly close: {latest_row['close']:.6f} at {latest_row['ts_hour']}")

    return {
        "mode": mode,
        "storage_backend": storage_backend,
        "effective_start": effective_start_iso,
        "db_existing_latest_ts": db_existing_latest_ts_iso,
        "db_latest_ts": db_latest_ts_iso,
        "raw_existing_rows": raw_existing_rows,
        "raw_new_rows": raw_new_rows,
        "raw_rows": int(len(merged_raw)),
        "hourly_existing_rows": hourly_existing_rows,
        "hourly_new_rows": hourly_new_rows,
        "hourly_upserted_rows": hourly_upserted_rows,
        "hourly_rows": int(hourly_rows),
        "raw_out": raw_out,
        "hourly_out": hourly_out,
        "latest_close": float(latest_row["close"]),
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
        update_overlap_hours=args.update_overlap_hours,
        storage_backend=args.storage_backend,
        pg_env_prefix=args.pg_env_prefix,
        pg_schema=args.pg_schema,
        pg_table=args.pg_table,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
