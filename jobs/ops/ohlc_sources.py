"""Dagster ops for OHLC data-source pipelines."""

from __future__ import annotations

import json

from dagster import Failure, Field, Nothing, Out, op

from jobs.ohlc_config import build_run_config_from_ohlc_config, iter_enabled_assets
from sources.chainlink.ohlc import run_chainlink_ohlc


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
    },
)
def chainlink_ohlc(context) -> None:
    cfg = context.op_config
    result = run_chainlink_ohlc(
        rpc_url=cfg["rpc_url"],
        feed=cfg["feed"],
        start=cfg["start"],
        raw_out=cfg["raw_out"],
        hourly_out=cfg["hourly_out"],
        max_staleness_sec=int(cfg["max_staleness_sec"]),
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
            op_cfg = build_run_config_from_ohlc_config(config_path)["ops"]["chainlink_ohlc"]["config"]
            run_result = run_chainlink_ohlc(
                rpc_url=str(op_cfg["rpc_url"]),
                feed=str(op_cfg["feed"]),
                start=str(op_cfg["start"]),
                raw_out=str(op_cfg["raw_out"]),
                hourly_out=str(op_cfg["hourly_out"]),
                max_staleness_sec=int(op_cfg["max_staleness_sec"]),
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
