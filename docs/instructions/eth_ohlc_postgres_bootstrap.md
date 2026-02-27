# ETH OHLC Postgres Bootstrap Runbook

This runbook bootstraps ETH hourly OHLC storage into Saturn Postgres and validates the data load.

## 1) Provision DB/table in storage repo

From Saturn host:

```bash
docker exec -i compose-postgres-1 \
  psql -v ON_ERROR_STOP=1 \
  -U mlflow \
  -d postgres \
  -v investing_rw_password='<strong-password>' \
  -f /dev/stdin < /home/carlos/storage/services/dagster/sql/investing_init.sql
```

Expected resources:
- DB: `investing`
- Role: `investing_rw`
- Table: `public.eth_ohlc_hourly`

## 2) Configure Dagster code location env

Add to `/home/carlos/storage/services/dagster/.env`:

```dotenv
INVESTING_PG_HOST=postgres
INVESTING_PG_PORT=5432
INVESTING_PG_DB=investing
INVESTING_PG_USER=investing_rw
INVESTING_PG_PASSWORD=<strong-password>
```

Reload services:

```bash
docker compose -f /home/carlos/storage/services/dagster/docker-compose.yml up -d
```

## 3) Bootstrap data from legacy parquet files

Run from investing code container:

```bash
docker exec dagster-investing_code_dev-1 /bin/sh -lc \
  \"python /opt/code/sources/migrations/bootstrap_eth_ohlc_to_postgres.py \
    --old-parquet /opt/code/data/ethusd_hourly.parquet \
    --new-parquet /opt/code/data/eth_ohlc_hourly.parquet \
    --pg-env-prefix INVESTING_PG \
    --pg-schema public \
    --pg-table eth_ohlc_hourly\"
```

## 4) Validate

Check table stats:

```bash
docker exec compose-postgres-1 psql -U mlflow -d investing -c \
  \"SELECT COUNT(*) AS rows, MIN(ts_hour) AS min_ts, MAX(ts_hour) AS max_ts FROM public.eth_ohlc_hourly;\"
```

Check duplicates:

```bash
docker exec compose-postgres-1 psql -U mlflow -d investing -c \
  \"SELECT ts_hour, COUNT(*) FROM public.eth_ohlc_hourly GROUP BY ts_hour HAVING COUNT(*) > 1;\"
```

## 5) Runtime updates

Dagster job `build_eth_ohlc_data` performs incremental upserts:
- watermark source: `MAX(ts_hour)` from `public.eth_ohlc_hourly`
- overlap: `pipeline.update_overlap_hours`
- dedupe key: `ts_hour` (`ON CONFLICT DO UPDATE`)
