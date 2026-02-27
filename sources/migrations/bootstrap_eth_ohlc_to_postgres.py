#!/usr/bin/env python3
"""Bootstrap ETH OHLC hourly table in Postgres from legacy parquet files."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values


HOURLY_COLS = ["ts_hour", "open", "high", "low", "close", "n_updates", "staleness_sec", "is_gap"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bootstrap public.eth_ohlc_hourly from legacy parquet files with latest-wins dedupe."
    )
    parser.add_argument("--old-parquet", default="data/ethusd_hourly.parquet", help="Legacy historical parquet path.")
    parser.add_argument("--new-parquet", default="data/eth_ohlc_hourly.parquet", help="Current parquet path.")
    parser.add_argument("--pg-env-prefix", default="INVESTING_PG", help="Env prefix for PG credentials.")
    parser.add_argument("--pg-schema", default="public", help="Target schema.")
    parser.add_argument("--pg-table", default="eth_ohlc_hourly", help="Target table.")
    return parser.parse_args()


def _load_pg_creds(prefix: str) -> dict[str, str | int]:
    pfx = prefix.strip().upper()
    if not pfx:
        raise RuntimeError("pg-env-prefix must be non-empty.")

    host = os.getenv(f"{pfx}_HOST", "").strip()
    port_raw = os.getenv(f"{pfx}_PORT", "5432").strip()
    dbname = os.getenv(f"{pfx}_DB", "").strip()
    user = os.getenv(f"{pfx}_USER", "").strip()
    password = os.getenv(f"{pfx}_PASSWORD", "").strip()

    missing = [
        key
        for key, value in [
            (f"{pfx}_HOST", host),
            (f"{pfx}_PORT", port_raw),
            (f"{pfx}_DB", dbname),
            (f"{pfx}_USER", user),
            (f"{pfx}_PASSWORD", password),
        ]
        if not value
    ]
    if missing:
        missing_fmt = ", ".join(missing)
        raise RuntimeError(f"Missing required Postgres env vars: {missing_fmt}")

    try:
        port = int(port_raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid integer for {pfx}_PORT: {port_raw}") from exc

    return {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password,
    }


def _normalize_hourly(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    missing = set(HOURLY_COLS) - set(df.columns)
    if missing:
        missing_fmt = ", ".join(sorted(missing))
        raise RuntimeError(f"{source_name} missing expected columns: {missing_fmt}")

    out = df[HOURLY_COLS].copy()
    out["ts_hour"] = pd.to_datetime(out["ts_hour"], utc=True, errors="coerce")
    out = out.dropna(subset=["ts_hour"])
    out["open"] = out["open"].astype(float)
    out["high"] = out["high"].astype(float)
    out["low"] = out["low"].astype(float)
    out["close"] = out["close"].astype(float)
    out["n_updates"] = out["n_updates"].fillna(0).astype(int)
    out["staleness_sec"] = pd.to_numeric(out["staleness_sec"], errors="coerce")
    out["is_gap"] = out["is_gap"].astype(bool)
    return out


def _ensure_table(conn, schema: str, table: str) -> None:
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


def _upsert_hourly(conn, schema: str, table: str, df: pd.DataFrame) -> int:
    schema_name = schema.strip() or "public"
    table_name = table.strip() or "eth_ohlc_hourly"

    rows: list[tuple[object, ...]] = []
    for row in df.itertuples(index=False):
        staleness = None if pd.isna(row.staleness_sec) else float(row.staleness_sec)
        rows.append(
            (
                row.ts_hour.to_pydatetime(),
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


def _fetch_stats(conn, schema: str, table: str) -> tuple[int, object, object, int]:
    schema_name = schema.strip() or "public"
    table_name = table.strip() or "eth_ohlc_hourly"

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT
                    COUNT(*) AS n_rows,
                    MIN(ts_hour) AS min_ts,
                    MAX(ts_hour) AS max_ts,
                    (COUNT(*) - COUNT(DISTINCT ts_hour)) AS dupes
                FROM {}.{}
                """
            ).format(sql.Identifier(schema_name), sql.Identifier(table_name))
        )
        row = cur.fetchone()

    return int(row[0]), row[1], row[2], int(row[3])


def main() -> int:
    args = parse_args()
    old_path = Path(args.old_parquet)
    new_path = Path(args.new_parquet)
    if not old_path.exists():
        raise FileNotFoundError(f"Missing old parquet: {old_path}")
    if not new_path.exists():
        raise FileNotFoundError(f"Missing new parquet: {new_path}")

    df_old = _normalize_hourly(pd.read_parquet(old_path), source_name=str(old_path))
    df_new = _normalize_hourly(pd.read_parquet(new_path), source_name=str(new_path))

    merged = pd.concat([df_old, df_new], ignore_index=True)
    merged = merged.drop_duplicates(subset=["ts_hour"], keep="last")
    merged = merged.sort_values("ts_hour", kind="mergesort").reset_index(drop=True)

    expected_rows = int(len(merged))
    expected_min_ts = merged["ts_hour"].min()
    expected_max_ts = merged["ts_hour"].max()
    print(
        f"Merged bootstrap dataset rows={expected_rows} min_ts={expected_min_ts} "
        f"max_ts={expected_max_ts} old_rows={len(df_old)} new_rows={len(df_new)}"
    )

    creds = _load_pg_creds(args.pg_env_prefix)
    conn = psycopg2.connect(
        host=str(creds["host"]),
        port=int(creds["port"]),
        dbname=str(creds["dbname"]),
        user=str(creds["user"]),
        password=str(creds["password"]),
    )
    try:
        _ensure_table(conn, args.pg_schema, args.pg_table)
        upserted = _upsert_hourly(conn, args.pg_schema, args.pg_table, merged)
        db_rows, db_min_ts, db_max_ts, db_dupes = _fetch_stats(conn, args.pg_schema, args.pg_table)
    finally:
        conn.close()

    print(f"Upserted rows from merged payload: {upserted}")
    print(f"Table stats rows={db_rows} min_ts={db_min_ts} max_ts={db_max_ts} duplicates={db_dupes}")

    if db_dupes != 0:
        raise RuntimeError(f"Expected 0 duplicates by ts_hour; found {db_dupes}")
    if db_rows != expected_rows:
        raise RuntimeError(f"Expected row count {expected_rows}, got {db_rows}")
    if pd.Timestamp(db_min_ts).tz_convert("UTC") != expected_min_ts:
        raise RuntimeError(f"Expected min ts {expected_min_ts}, got {db_min_ts}")
    if pd.Timestamp(db_max_ts).tz_convert("UTC") != expected_max_ts:
        raise RuntimeError(f"Expected max ts {expected_max_ts}, got {db_max_ts}")

    print("Bootstrap validation successful.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
