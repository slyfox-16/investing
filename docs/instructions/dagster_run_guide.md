# Run `build_eth_training` In Dagster

## 1) Start Dagster

From repo root:

```bash
dagster dev -m jobs
```

Dagster loads `jobs.defs` from `jobs/__init__.py`.

## 2) Open Launchpad

In the Dagster UI:

1. Go to job: `build_eth_training`
2. Click `Launchpad`
3. Use either empty config (default YAML path) or optional path override below
4. Click `Launch Run`

## 3) Minimal Launchpad Config (Recommended)

```yaml
{}
```

This loads `configs/ETH/training_data.yaml` automatically (or `TRAINING_DATA_CONFIG` if set).

## 4) Optional Launchpad Config: override YAML path

```yaml
training_config_path: "/Users/carlos.ortega/investing/configs/ETH/training_data.yaml"
```

## 5) Notes

- Ensure `INFURA_HTTP` is set in your environment (or replace `rpc_url` with a literal URL).
- Paths inside `training_data.yaml` are relative to repo root unless absolute.
- You no longer need to paste per-op config in Launchpad.
