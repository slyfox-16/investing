# ETH Data Pipeline Flow (One Page)

```mermaid
flowchart TD
    A[Chainlink ETH/USD Feed\ngetRoundData/latestRoundData] --> B[sources.chainlink.eth_hourly]
    B --> B1[data/chainlink_rounds.parquet\n(raw rounds)]
    B --> B2[data/ethusd_hourly.parquet\n(hourly spot OHLC + staleness)]

    C[Deribit DVOL API\npublic/get_volatility_index_data] --> D[sources.deribit.eth_iv_index]
    D --> D1[data/deribit_eth_dvol_raw.parquet\n(raw IV index candles)]
    D --> D2[data/eth_iv_index_hourly.parquet\n(hourly IV index features)]

    E[Deribit Options APIs\nget_instruments + get_book_summary_by_currency] --> F[sources.deribit.eth_iv_surface]
    F --> F1[data/deribit_eth_option_surface_raw.parquet\n(raw option snapshot rows)]
    F --> F2[data/eth_iv_surface_hourly.parquet\n(hourly IV surface features)]

    B2 --> G[sources.build_eth_training_dataset]
    D2 --> G
    F2 --> G

    G --> H[data/eth_training_hourly.parquet\n(final training dataset)]

    H --> T1[Targets added\nret_1h, ret_6h, ret_24h]
    H --> T2[Targets added\nrv_6h, rv_24h]
```

## What each stage does

- `sources.chainlink.eth_hourly`
  - Pulls Chainlink round-level ETH/USD data.
  - Writes raw rounds and hourly spot bars.

- `sources.deribit.eth_iv_index`
  - Pulls Deribit DVOL candles.
  - Writes raw candles and hourly normalized IV-index features.

- `sources.deribit.eth_iv_surface`
  - Pulls Deribit option-chain snapshot data.
  - Writes raw per-instrument snapshot rows and hourly engineered surface features.

- `sources.build_eth_training_dataset`
  - Left-joins hourly spot with hourly IV index and hourly IV surface on `ts_hour`.
  - Adds forward return and future realized-volatility targets.

## Raw vs Hourly (quick difference)

- `raw` files: closer to source payloads, higher granularity, minimal feature engineering.
- `hourly` files: standardized 1-hour buckets (`ts_hour`) designed for model-ready joins.
