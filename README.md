# investing

OHLC data pipeline with Dagster pull jobs and notebook-driven time-series analysis.

## ENV Variables
* 'PROJECT_ROOT'
* 'INFURA_API_KEY'
* 'INFURA_HTTP'
* 'OHLC_DATA_CONFIG' (optional; default single-asset config path)
* 'OHLC_MASTER_CONFIG' (optional; default master multi-asset config path)

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

1. Step 1 (`Dagster`): pull and store OHLC parquet data.
   - Single asset: `build_eth_ohlc_data`
   - Multi asset: `build_all_ohlc_data`
2. Step 2 (`Notebook`): run time-series diagnostics.
   - `notebooks/time_series_analysis.ipynb`
   - Calls `models.analysis.ohlc_time_series_analysis(asset=...)`

Default configs:
- Single-asset ETH config: `configs/ETH/ohlc_data.yaml`
- Master config: `configs/ohlc_master.yaml`

## Saturn deployment (external code location)

To expose this repo to Saturn Dagster as a gRPC code location, use:

- `Dockerfile.dagster-code-server`
- `docs/instructions/saturn_external_code_location.md`
