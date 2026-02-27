# OHLC Pipeline DataFrame Reference

This document explains the OHLC datasets produced by the pull pipeline.

All timestamps are UTC.

## 1) `<asset>_chainlink_rounds_df` (`<asset>_chainlink_rounds.parquet`)

What it represents:
- Raw Chainlink rounds pulled from `getRoundData` / `latestRoundData`.
- One row per feed update round.

Columns:
- `round_id`: Chainlink round id (`uint80`, persisted as string).
- `answer`: Raw integer feed answer.
- `price`: Decoded price (`answer / 10**decimals`).
- `started_at`: Round start timestamp.
- `updated_at`: Round update timestamp.
- `answered_in_round`: Answer round id (`uint80`, persisted as string).

## 2) `eth_ohlc_hourly` (Postgres table)

What it represents:
- Hourly OHLC bars derived from raw Chainlink rounds.
- For ETH, default sink is Postgres table `public.eth_ohlc_hourly`.

Grain:
- 1 row per hour (`ts_hour`).

Columns:
- `ts_hour`: Hour bucket timestamp.
- `open`: First price in hour (or filled from `close` for empty hours).
- `high`: Max price in hour (or filled from `close` for empty hours).
- `low`: Min price in hour (or filled from `close` for empty hours).
- `close`: Last price in hour; forward-filled across empty hours.
- `n_updates`: Count of feed updates in the hour.
- `staleness_sec`: Seconds between hour-end and latest update at/before hour-end.
- `is_gap`: True when `staleness_sec > max_staleness_sec`.
- `ingested_at`: Insert timestamp (defaults to `now()`).
- `updated_at_ingest`: Last upsert timestamp.

## Notes

- Asset-specific file names come from `configs/<ASSET>/ohlc_data.yaml`.
- Output paths are relative to `pipeline.data_dir` unless absolute paths are provided.
- `pipeline.storage.backend` controls hourly sink (`postgres` or `parquet`).
- Pipeline write mode is incremental append/upsert with dedupe (`latest pull wins`):
  - Raw rounds: dedupe key `round_id`
  - Hourly bars: dedupe key `ts_hour`
