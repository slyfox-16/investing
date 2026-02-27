# OHLC Data Pipeline Flow (One Page)

```mermaid
flowchart TD
    A[Chainlink Feed\ngetRoundData/latestRoundData] --> B[sources.chainlink.ohlc]
    B --> B1[data/<asset>_chainlink_rounds.parquet\n(raw rounds)]
    B --> B2[(Postgres\n<schema>.<table>\n(hourly OHLC + staleness))]

    C[jobs.pipelines.eth_ohlc\nbuild_eth_ohlc_data] --> B
    D[jobs.pipelines.all_ohlc\nbuild_all_ohlc_data] --> C
```

## What each stage does

- `sources.chainlink.ohlc`
  - Pulls round-level data from the configured Chainlink feed.
  - Uses bootstrap + incremental watermarking:
    - Bootstrap: start from configured `pipeline.start`.
    - Incremental: start from `latest ts_hour - update_overlap_hours` (bounded by `pipeline.start`), where latest is read from the configured storage backend.
  - Writes raw rounds to parquet via merge + dedupe (`latest pull wins`).
  - Writes hourly rows to:
    - Postgres (`pipeline.storage.backend=postgres`) via upsert on `ts_hour`, or
    - parquet (`pipeline.storage.backend=parquet`) via merge + dedupe.

- `jobs.pipelines.eth_ohlc`
  - Runs a single-asset OHLC pull using `configs/<ASSET>/ohlc_data.yaml`.

- `jobs.pipelines.all_ohlc`
  - Runs multiple asset pulls from `configs/ohlc_master.yaml`.
  - Continues through all assets and fails at end if any asset run fails.

- `jobs.schedules.eth_ohlc_daily_schedule`
  - Runs `build_eth_ohlc_data` daily at `00:15 UTC` (`15 0 * * *`).

## Raw vs Hourly (quick difference)

- `raw` files: closer to source payloads, higher granularity, minimal feature engineering.
- `hourly` files: standardized 1-hour OHLC buckets (`ts_hour`) for analysis/modeling.
