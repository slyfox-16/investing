"""Helpers for loading OHLC datasets for analysis."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
from typing import Any, Mapping, cast

import pandas as pd
from dotenv import load_dotenv

from jobs.ohlc_config import REPO_ROOT, default_asset_config_path, load_ohlc_config


def _as_mapping(value: Any, field_name: str) -> dict[str, object]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"Invalid '{field_name}' config; expected mapping.")
    return cast(dict[str, object], value)


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise ValueError(f"Missing environment variable: {name}")
    return value.strip()


def _load_runtime_env() -> None:
    # Load optional env files for notebook/local execution contexts.
    candidate_paths = [
        os.getenv("OHLC_ENV_FILE", ""),
        os.getenv("SATURN_DAGSTER_ENV_FILE", ""),
        str(REPO_ROOT / ".env"),
        str(REPO_ROOT.parent / "storage" / "services" / "dagster" / ".env"),
    ]
    for candidate in candidate_paths:
        if not candidate:
            continue
        p = Path(candidate).expanduser()
        if p.exists() and p.is_file():
            load_dotenv(p, override=False)


def _detect_compose_postgres_ip() -> str | None:
    try:
        proc = subprocess.run(
            ["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}", "compose-postgres-1"],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:  # noqa: BLE001
        return None

    out = proc.stdout.strip()
    if not out:
        return None
    ip = out.split()[0].strip()
    return ip or None


def _load_ohlc_data_from_postgres(storage_cfg: Mapping[str, object]) -> pd.DataFrame:
    try:
        import psycopg2
        from psycopg2 import sql
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "Postgres backend requested but psycopg2 is not available in this environment."
        ) from exc

    pg_env_prefix = str(storage_cfg.get("db_env_prefix", "INVESTING_PG")).strip().upper()
    pg_schema = str(storage_cfg.get("schema", "public")).strip() or "public"
    pg_table = str(storage_cfg.get("table", "eth_ohlc_hourly")).strip() or "eth_ohlc_hourly"
    if not pg_env_prefix:
        raise ValueError("pipeline.storage.db_env_prefix must be non-empty for postgres backend.")

    _load_runtime_env()
    host = _require_env(f"{pg_env_prefix}_HOST")
    port_raw = _require_env(f"{pg_env_prefix}_PORT")
    dbname = _require_env(f"{pg_env_prefix}_DB")
    user = _require_env(f"{pg_env_prefix}_USER")
    password = _require_env(f"{pg_env_prefix}_PASSWORD")

    port = int(port_raw)
    query = sql.SQL(
        "SELECT ts_hour, open, high, low, close, n_updates, staleness_sec, is_gap "
        "FROM {}.{} ORDER BY ts_hour"
    ).format(sql.Identifier(pg_schema), sql.Identifier(pg_table))
    print(f"Loading OHLC data from Postgres table {pg_schema}.{pg_table}.")

    connect_kwargs = {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password,
    }
    try:
        conn = psycopg2.connect(**connect_kwargs)
    except psycopg2.OperationalError as exc:
        msg = str(exc).lower()
        if host == "postgres" and "could not translate host name" in msg:
            fallback_candidates: list[tuple[str, int]] = []
            host_local = os.getenv(f"{pg_env_prefix}_HOST_LOCAL", "").strip()
            port_local_raw = os.getenv(f"{pg_env_prefix}_PORT_LOCAL", "").strip()
            if host_local:
                try:
                    port_local = int(port_local_raw) if port_local_raw else port
                except ValueError as ve:
                    raise ValueError(f"Invalid integer in {pg_env_prefix}_PORT_LOCAL: {port_local_raw}") from ve
                fallback_candidates.append((host_local, port_local))

            docker_ip = _detect_compose_postgres_ip()
            if docker_ip:
                fallback_candidates.append((docker_ip, port))

            if not fallback_candidates:
                raise RuntimeError(
                    "Postgres host 'postgres' is not resolvable from this notebook runtime. "
                    f"Set {pg_env_prefix}_HOST_LOCAL (and optional {pg_env_prefix}_PORT_LOCAL), "
                    "or run notebook in the Dagster code container."
                ) from exc

            last_exc: Exception = exc
            conn = None
            for fb_host, fb_port in fallback_candidates:
                if fb_host == host and fb_port == port:
                    continue
                print(
                    f"Host '{host}' is not resolvable in this runtime; retrying Postgres with "
                    f"host='{fb_host}', port={fb_port}."
                )
                try:
                    connect_kwargs["host"] = fb_host
                    connect_kwargs["port"] = fb_port
                    conn = psycopg2.connect(**connect_kwargs)
                    break
                except psycopg2.OperationalError as fb_exc:
                    last_exc = fb_exc
            if conn is None:
                raise last_exc
        else:
            raise

    with conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            description = cur.description
            if description is None:
                raise RuntimeError("Database cursor returned no result metadata for OHLC query.")
            cols = [d.name for d in description]

    out = pd.DataFrame(rows, columns=cols)
    if not out.empty:
        out["ts_hour"] = pd.to_datetime(out["ts_hour"], utc=True, errors="coerce")
    return out


def load_ohlc_data(asset: str, config_path: str | Path | None = None) -> pd.DataFrame:
    """Load configured OHLC data for an asset as a pandas DataFrame."""
    asset_id = asset.strip().lower()
    if not asset_id:
        raise ValueError("Asset id must be non-empty.")

    resolved_config_path = Path(config_path) if config_path else default_asset_config_path(asset_id)
    cfg = load_ohlc_config(resolved_config_path)
    pipeline_cfg = _as_mapping(cfg.get("pipeline"), "pipeline")
    storage_cfg = _as_mapping(pipeline_cfg.get("storage"), "pipeline.storage")
    backend = str(storage_cfg.get("backend", "parquet")).strip().lower()

    if backend == "postgres":
        return _load_ohlc_data_from_postgres(storage_cfg)
    if backend != "parquet":
        raise ValueError(f"Unsupported pipeline.storage.backend '{backend}'.")
    print(f"Loading OHLC data from parquet backend for asset '{asset_id}'.")

    data_dir = Path(str(pipeline_cfg.get("data_dir", "data")))
    if not data_dir.is_absolute():
        data_dir = REPO_ROOT / data_dir

    outputs = _as_mapping(pipeline_cfg.get("outputs"), "pipeline.outputs")
    ohlc_out = Path(str(outputs.get("ohlc_hourly", f"{asset_id}_ohlc_hourly.parquet")))
    ohlc_path = ohlc_out if ohlc_out.is_absolute() else (data_dir / ohlc_out)

    if not ohlc_path.exists():
        raise FileNotFoundError(f"OHLC parquet not found for asset '{asset_id}': {ohlc_path}")

    return pd.read_parquet(ohlc_path)
