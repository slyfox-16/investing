# investing

OHLC data pipeline with Dagster pull jobs and notebook-driven time-series analysis.

## ENV Variables
* 'PROJECT_ROOT'
* 'INFURA_API_KEY'
* 'INFURA_HTTP'
* 'OHLC_DATA_CONFIG' (optional; default single-asset config path)
* 'OHLC_MASTER_CONFIG' (optional; default master multi-asset config path)
* 'INVESTING_PG_HOST' (when `pipeline.storage.backend=postgres`)
* 'INVESTING_PG_PORT'
* 'INVESTING_PG_DB'
* 'INVESTING_PG_USER'
* 'INVESTING_PG_PASSWORD'

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Dagster entrypoint

Use `jobs` as the repository entrypoint:

```bash
dagster dev -m jobs
```

## Two-Step Workflow

1. Step 1 (`Dagster`): pull and store OHLC data (Postgres for ETH hourly, parquet for raw rounds).
   - Single asset: `build_eth_ohlc_data`
   - Multi asset: `build_all_ohlc_data`
   - Schedule: `eth_ohlc_daily_schedule` (`15 0 * * *`, UTC)
2. Step 2 (`Notebook`): run time-series diagnostics.
   - `notebooks/time_series_analysis.ipynb`
   - Calls `models.analysis.ohlc_time_series_analysis(asset=...)`

## ETH OHLC Incremental Behavior

- `build_eth_ohlc_data` does bootstrap + incremental writes.
- Raw Chainlink rounds are persisted to parquet and deduped by `round_id` (`latest pull wins`).
- Hourly OHLC rows are persisted to Postgres (`pipeline.storage.backend=postgres` for ETH):
  - Incremental watermark uses `MAX(ts_hour)` from the target table.
  - Pull starts from `max(pipeline.start, max_ts_hour - pipeline.update_overlap_hours)`.
  - Rows are upserted by `ts_hour` (`latest pull wins` via `ON CONFLICT DO UPDATE`).

Default configs:
- Single-asset ETH config: `configs/ETH/ohlc_data.yaml`
- Master config: `configs/ohlc_master.yaml`

## Saturn deployment (external code location)

To expose this repo to Saturn Dagster as a gRPC code location, use:

- `Dockerfile.dagster-code-server`
- `docs/instructions/saturn_external_code_location.md`
- `docs/instructions/eth_ohlc_postgres_bootstrap.md`
