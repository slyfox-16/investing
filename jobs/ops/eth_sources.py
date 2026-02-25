"""Dagster ops that execute ETH data-source modules."""

from __future__ import annotations

from dagster import In, Nothing, Out, op

from sources.build_eth_training_dataset import run_build_eth_training_dataset
from sources.chainlink.eth_hourly import run_chainlink_eth_hourly
from sources.deribit.eth_iv_index import run_deribit_eth_iv_index
from sources.deribit.eth_iv_surface import run_deribit_eth_iv_surface


@op(
    name="ethusd_hourly",
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
def ethusd_hourly(context) -> None:
    cfg = context.op_config
    result = run_chainlink_eth_hourly(
        rpc_url=cfg["rpc_url"],
        feed=cfg["feed"],
        start=cfg["start"],
        raw_out=cfg["raw_out"],
        hourly_out=cfg["hourly_out"],
        max_staleness_sec=int(cfg["max_staleness_sec"]),
    )
    context.log.info(f"Chainlink complete: {result}")


@op(
    name="eth_IV_index",
    ins={"start_after": In(Nothing)},
    out=Out(Nothing),
    config_schema={
        "base_url": str,
        "currency": str,
        "resolution": str,
        "start": str,
        "chunk_hours": int,
        "sleep_sec": float,
        "timeout_sec": int,
        "raw_out": str,
        "hourly_out": str,
    },
)
def eth_iv_index(context) -> None:
    cfg = context.op_config
    result = run_deribit_eth_iv_index(
        base_url=cfg["base_url"],
        currency=cfg["currency"],
        resolution=cfg["resolution"],
        start=cfg["start"],
        chunk_hours=int(cfg["chunk_hours"]),
        sleep_sec=float(cfg["sleep_sec"]),
        timeout_sec=int(cfg["timeout_sec"]),
        raw_out_path=cfg["raw_out"],
        hourly_out_path=cfg["hourly_out"],
    )
    context.log.info(f"Deribit IV index complete: {result}")


@op(
    name="eth_iv_surface",
    ins={"start_after": In(Nothing)},
    out=Out(Nothing),
    config_schema={
        "base_url": str,
        "currency": str,
        "timeout_sec": int,
        "surface_raw_out": str,
        "surface_hourly_out": str,
        "training_out": str,
        "spot_hourly_in": str,
        "iv_index_hourly_in": str,
        "min_contracts": int,
        "min_total_oi": float,
        "max_staleness_hours": int,
    },
)
def eth_iv_surface(context) -> None:
    cfg = context.op_config
    surface_result = run_deribit_eth_iv_surface(
        base_url=cfg["base_url"],
        currency=cfg["currency"],
        timeout_sec=int(cfg["timeout_sec"]),
        raw_out_path=cfg["surface_raw_out"],
        hourly_out_path=cfg["surface_hourly_out"],
        min_contracts=int(cfg["min_contracts"]),
        min_total_oi=float(cfg["min_total_oi"]),
        max_staleness_hours=int(cfg["max_staleness_hours"]),
    )
    training_result = run_build_eth_training_dataset(
        spot_path=cfg["spot_hourly_in"],
        iv_index_path=cfg["iv_index_hourly_in"],
        iv_surface_path=cfg["surface_hourly_out"],
        out_path=cfg["training_out"],
    )
    context.log.info(f"Deribit IV surface complete: {surface_result}")
    context.log.info(f"Training dataset complete: {training_result}")
