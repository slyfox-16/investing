# ETH Pipeline DataFrame Reference

This document explains the parquet-backed DataFrames loaded in `notebooks/load_eth_parquets.ipynb`:

- `chainlink_rounds_df`
- `ethusd_hourly_df`
- `deribit_eth_dvol_raw_df`
- `eth_iv_index_hourly_df`
- `deribit_eth_option_surface_raw_df`
- `eth_iv_surface_hourly_df`
- `eth_training_hourly_df`

All timestamps are UTC.

## 1) `chainlink_rounds_df` (`chainlink_rounds.parquet`)

What it represents:
- Raw Chainlink ETH/USD feed rounds pulled from `getRoundData`/`latestRoundData` through the ETH/USD proxy feed.
- One row per Chainlink round observed during backfill.

Grain:
- Event-like (irregular): one row per oracle round update.

Columns:
- `round_id`: Chainlink round identifier (stored as string because proxy round IDs are `uint80` and may exceed signed 64-bit range).
- `answer`: Raw integer oracle answer from Chainlink.
- `price`: Decoded spot price, computed as `answer / (10 ** decimals)`.
- `started_at`: Round start timestamp.
- `updated_at`: Timestamp when round answer was last updated.
- `answered_in_round`: Round id in which the answer was computed (stored as string for the same uint80 reason).

## 2) `ethusd_hourly_df` (`ethusd_hourly.parquet`)

What it represents:
- Hourly ETH/USD OHLC bars derived from Chainlink round-level prices.
- This is the canonical spot table used as the left/base table in final model dataset joins.

Grain:
- 1 row per hour (`ts_hour`).

Columns:
- `ts_hour`: Hour bucket timestamp.
- `open`: First Chainlink price in that hour (or filled from `close` when no updates occurred in hour).
- `high`: Maximum Chainlink price in that hour (or filled from `close` for empty hours).
- `low`: Minimum Chainlink price in that hour (or filled from `close` for empty hours).
- `close`: Last Chainlink price in that hour; forward-filled across empty hours.
- `n_updates`: Count of Chainlink updates that landed in the hour.
- `staleness_sec`: Seconds between hour-end (`ts_hour + 1h`) and most recent feed update at or before hour-end.
- `is_gap`: `True` when `staleness_sec > max_staleness_sec` (default threshold: 7200 seconds).

## 3) `deribit_eth_dvol_raw_df` (`deribit_eth_dvol_raw.parquet`)

What it represents:
- Raw Deribit ETH volatility index candles from `public/get_volatility_index_data`.
- Values are normalized to decimal volatility form when needed.

Grain:
- API candle resolution (configured with `--resolution`, default 60 minutes).

Columns:
- `ts`: Candle timestamp from Deribit.
- `open`: Vol index open (decimal vol; e.g., `0.65` means 65%).
- `high`: Vol index high.
- `low`: Vol index low.
- `close`: Vol index close.
- `source`: Static lineage tag (`deribit_dvol`).
- `ingested_at`: Pipeline ingestion timestamp.
- `api_endpoint_version`: Static API version tag (`deribit_api_v2`).

## 4) `eth_iv_index_hourly_df` (`eth_iv_index_hourly.parquet`)

What it represents:
- Hourly-normalized Deribit DVOL index feature table.
- Derived by resampling raw index candles to hourly OHLC.

Grain:
- 1 row per hour (`ts_hour`).

Columns:
- `ts_hour`: Hour bucket timestamp.
- `iv_index_open`: First index value in hour.
- `iv_index_high`: Maximum index value in hour.
- `iv_index_low`: Minimum index value in hour.
- `iv_index_close`: Last index value in hour.
- `source`: Static lineage tag (`deribit_dvol`).
- `ingested_at`: Ingestion timestamp.
- `api_endpoint_version`: Static API version tag (`deribit_api_v2`).
- `quality_flag`: Currently set to `ok` by the builder.

## 5) `deribit_eth_option_surface_raw_df` (`deribit_eth_option_surface_raw.parquet`)

What it represents:
- Raw option-chain snapshot table used to build surface features.
- Built by joining Deribit instrument metadata (`public/get_instruments`) with summary fields (`public/get_book_summary_by_currency`).
- One row per option instrument per snapshot time.

Grain:
- Snapshot-instrument level.

Columns:
- `snapshot_ts`: Snapshot timestamp when data was fetched.
- `instrument_name`: Deribit instrument code (for example, `ETH-28MAR25-3000-C`).
- `expiry_ts`: Option expiry timestamp.
- `strike`: Strike price.
- `option_type`: `call` or `put`.
- `mark_iv`: Mark implied volatility (decimal form).
- `underlying_price`: Underlying/index price used for option metrics.
- `open_interest`: Outstanding contracts (in underlying units for options on Deribit docs).
- `bid_price`: Best bid price.
- `ask_price`: Best ask price.
- `mark_price`: Mark price.
- `source`: Static lineage tag (`deribit_option_summary`).
- `ingested_at`: Ingestion timestamp.
- `api_endpoint_version`: Static API version tag (`deribit_api_v2`).

## 6) `eth_iv_surface_hourly_df` (`eth_iv_surface_hourly.parquet`)

What it represents:
- Engineered hourly option-surface features derived from the raw snapshot table.
- Includes term-structure and skew metrics around ATM and ~30D tenor, plus data quality controls.

Grain:
- 1 row per hour (`ts_hour`).

Columns:
- `ts_hour`: Hour bucket timestamp.
- `iv_atm_7d`: ATM IV interpolated to 7-day tenor.
- `iv_atm_14d`: ATM IV interpolated to 14-day tenor.
- `iv_atm_30d`: ATM IV interpolated to 30-day tenor.
- `iv_atm_60d`: ATM IV interpolated to 60-day tenor.
- `rr_25d_30d`: 25-delta style risk reversal proxy at ~30D: `IV(call, moneyness~1.1) - IV(put, moneyness~0.9)`.
- `bf_25d_30d`: 25-delta style butterfly proxy at ~30D: `0.5 * (IV_call_25 + IV_put_25) - IV_atm`.
- `atm_term_slope_7d_30d`: Slope of ATM term structure between 7D and 30D.
- `atm_term_curvature_7d_30d_60d`: Curvature proxy `IV60 - 2*IV30 + IV7`.
- `n_contracts_used`: Number of valid option rows used for that hour's feature computation.
- `oi_weighted_iv_30d`: Open-interest-weighted IV for contracts with days-to-expiry roughly in [23, 37].
- `source`: Static lineage tag (`deribit_option_surface`).
- `ingested_at`: Ingestion timestamp.
- `api_endpoint_version`: Static API version tag (`deribit_api_v2`).
- `quality_flag`: Quality state:
  - `ok`: Enough contracts/open-interest and core tenors available.
  - `insufficient_liquidity`: Not enough valid data.
  - `stale_fill`: Hour had no fresh snapshot but feature columns were forward-filled (bounded by `max_staleness_hours`).

## 7) `eth_training_hourly_df` (`eth_training_hourly.parquet`)

What it represents:
- Final model-ready hourly table.
- Built as left join of:
  1. spot base (`ethusd_hourly_df`)
  2. IV index features (`eth_iv_index_hourly_df`)
  3. IV surface features (`eth_iv_surface_hourly_df`)
- Plus forward-return and realized-volatility targets.

Grain:
- 1 row per hour, aligned to spot table `ts_hour`.

Columns:
- Spot block (from `ethusd_hourly_df`):
  - `ts_hour`, `open`, `high`, `low`, `close`, `n_updates`, `staleness_sec`, `is_gap`
- IV index block (from `eth_iv_index_hourly_df`):
  - `iv_index_open`, `iv_index_high`, `iv_index_low`, `iv_index_close`, `source`, `ingested_at`, `api_endpoint_version`, `quality_flag`
- IV surface block (from `eth_iv_surface_hourly_df`):
  - `iv_atm_7d`, `iv_atm_14d`, `iv_atm_30d`, `iv_atm_60d`, `rr_25d_30d`, `bf_25d_30d`, `atm_term_slope_7d_30d`, `atm_term_curvature_7d_30d_60d`, `n_contracts_used`, `oi_weighted_iv_30d`, `source_ivsurf`, `ingested_at_ivsurf`, `api_endpoint_version_ivsurf`, `quality_flag_ivsurf`
- Targets:
  - `ret_1h`: Forward 1-hour return `close[t+1]/close[t] - 1`.
  - `ret_6h`: Forward 6-hour return.
  - `ret_24h`: Forward 24-hour return.
  - `rv_6h`: Future 6-hour realized vol, `sqrt(sum_{i=1..6} log_ret[t+i]^2)`.
  - `rv_24h`: Future 24-hour realized vol, `sqrt(sum_{i=1..24} log_ret[t+i]^2)`.

Notes:
- Because this is a left join on spot hours, IV fields can be null when no matching Deribit hour exists.
- `n_contracts_used` can become float in this final table due to nullability introduced by joins.

## External API semantics used

- Chainlink `AggregatorV3Interface` return fields (`roundId`, `answer`, `startedAt`, `updatedAt`, `answeredInRound`) are standard feed response fields.
- Deribit volatility index candles come from `public/get_volatility_index_data`.
- Deribit option summary fields (`mark_iv`, `underlying_price`, `open_interest`, bid/ask/mark) come from `public/get_book_summary_by_currency`.

