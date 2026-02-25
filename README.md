# investing

ETH forecasting data pipeline with Chainlink spot prices and Deribit implied-volatility features.

## ENV Variables
* 'PROJECT_ROOT'
* 'INFURA_API_KEY'
* 'INFURA_HTTP'

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

`build_eth_training` reads config from `configs/ETH/training_data.yaml` automatically.
Override with `TRAINING_DATA_CONFIG=/abs/path/to/training_data.yaml` when needed.

## Saturn deployment (external code location)

To expose this repo to Saturn Dagster as a gRPC code location, use:

- `Dockerfile.dagster-code-server`
- `docs/instructions/saturn_external_code_location.md`
