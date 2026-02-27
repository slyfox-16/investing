# Run OHLC Jobs In Dagster

## 1) Start Dagster

From repo root:

```bash
dagster dev -m jobs
```

Dagster loads `jobs.defs` from `jobs/__init__.py`.

## 2) Open Launchpad

In the Dagster UI:

1. Go to job: `build_eth_ohlc_data` (single asset) or `build_all_ohlc_data` (master)
2. Optional schedule: `eth_ohlc_daily_schedule` (daily at `00:15 UTC`)
3. Click `Launchpad`
4. Use either empty config (default YAML path) or optional path override below
5. Click `Launch Run`

## 3) Minimal Launchpad Config (Recommended)

```yaml
{}
```

For `build_eth_ohlc_data`, this loads `configs/ETH/ohlc_data.yaml` automatically
(or `OHLC_DATA_CONFIG` if set).

For `build_all_ohlc_data`, this loads `configs/ohlc_master.yaml` automatically
(or `OHLC_MASTER_CONFIG` if set).

## 4) Optional Launchpad Config: override YAML path (single asset)

```yaml
ohlc_config_path: "/Users/carlos.ortega/investing/configs/ETH/ohlc_data.yaml"
```

## 5) Optional Launchpad Config: override YAML path (master)

```yaml
master_config_path: "/Users/carlos.ortega/investing/configs/ohlc_master.yaml"
```

## 6) Notes

- Ensure `INFURA_HTTP` is set in your environment (or replace `rpc_url` with a literal URL).
- For Postgres hourly sink (`pipeline.storage.backend=postgres`), ensure:
  - `INVESTING_PG_HOST`
  - `INVESTING_PG_PORT`
  - `INVESTING_PG_DB`
  - `INVESTING_PG_USER`
  - `INVESTING_PG_PASSWORD`
- Paths inside `ohlc_data.yaml` are relative to repo root unless absolute.
- You no longer need to paste per-op config in Launchpad.
