"""Dagster schedules for ETH OHLC jobs."""

from dagster import ScheduleDefinition

from jobs.pipelines import build_eth_ohlc_data


eth_ohlc_daily_schedule = ScheduleDefinition(
    name="eth_ohlc_daily_schedule",
    job=build_eth_ohlc_data,
    cron_schedule="15 0 * * *",
    execution_timezone="UTC",
    run_config={},
)
