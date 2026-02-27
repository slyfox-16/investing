"""Dagster ops for OHLC data-source pipelines."""

from __future__ import annotations

import json
from typing import TypedDict, cast

from dagster import Failure, Field, Nothing, Out, op

from jobs.ohlc_config import build_run_config_from_ohlc_config, iter_enabled_assets
from sources.chainlink.ohlc import run_chainlink_ohlc


class ChainlinkOpConfig(TypedDict):
    rpc_url: str
    feed: str
    start: str
    raw_out: str
    hourly_out: str
    max_staleness_sec: int
    update_overlap_hours: int
    storage_backend: str
    pg_env_prefix: str
    pg_schema: str
    pg_table: str


@op(
    name="chainlink_ohlc",
    out=Out(Nothing),
    config_schema={
        "rpc_url": str,
        "feed": str,
        "start": str,
        "raw_out": str,
        "hourly_out": str,
        "max_staleness_sec": int,
        "update_overlap_hours": int,
        "storage_backend": str,
        "pg_env_prefix": str,
        "pg_schema": str,
        "pg_table": str,
    },
)
def chainlink_ohlc(context) -> None:
    cfg = cast(ChainlinkOpConfig, context.op_config)
    result = run_chainlink_ohlc(
        rpc_url=cfg["rpc_url"],
        feed=cfg["feed"],
        start=cfg["start"],
        raw_out=cfg["raw_out"],
        hourly_out=cfg["hourly_out"],
        max_staleness_sec=cfg["max_staleness_sec"],
        update_overlap_hours=cfg["update_overlap_hours"],
        storage_backend=cfg["storage_backend"],
        pg_env_prefix=cfg["pg_env_prefix"],
        pg_schema=cfg["pg_schema"],
        pg_table=cfg["pg_table"],
    )
    context.log.info(f"Chainlink OHLC complete: {result}")


@op(
    name="run_all_ohlc_assets",
    out=Out(Nothing),
    config_schema={
        "master_config_path": Field(str, is_required=False, default_value=""),
    },
)
def run_all_ohlc_assets(context) -> None:
    master_config_path = str(context.op_config.get("master_config_path", "")).strip() or None
    assets = iter_enabled_assets(master_config_path)
    if not assets:
        raise Failure("No enabled assets found in master config.")

    results: list[dict[str, object]] = []
    failed = False

    for asset in assets:
        asset_id = str(asset["id"])
        config_path = str(asset["config_path"])
        context.log.info(f"Running OHLC pull for asset={asset_id} config={config_path}")
        try:
            run_cfg = build_run_config_from_ohlc_config(config_path)
            ops_cfg = cast(dict[str, object], run_cfg["ops"])
            chainlink_cfg = cast(dict[str, object], ops_cfg["chainlink_ohlc"])
            op_cfg = cast(ChainlinkOpConfig, chainlink_cfg["config"])
            run_result = run_chainlink_ohlc(
                rpc_url=op_cfg["rpc_url"],
                feed=op_cfg["feed"],
                start=op_cfg["start"],
                raw_out=op_cfg["raw_out"],
                hourly_out=op_cfg["hourly_out"],
                max_staleness_sec=op_cfg["max_staleness_sec"],
                update_overlap_hours=op_cfg["update_overlap_hours"],
                storage_backend=op_cfg["storage_backend"],
                pg_env_prefix=op_cfg["pg_env_prefix"],
                pg_schema=op_cfg["pg_schema"],
                pg_table=op_cfg["pg_table"],
            )
            results.append({"asset": asset_id, "status": "ok", "result": run_result})
            context.log.info(f"Asset {asset_id} completed successfully.")
        except Exception as exc:  # noqa: BLE001
            failed = True
            results.append({"asset": asset_id, "status": "failed", "error": str(exc)})
            context.log.error(f"Asset {asset_id} failed: {exc}")

    context.log.info("Master OHLC summary:\n" + json.dumps(results, indent=2, sort_keys=True))
    if failed:
        raise Failure("One or more assets failed during build_all_ohlc_data. See logs for per-asset details.")
