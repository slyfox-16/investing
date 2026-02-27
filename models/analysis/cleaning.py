"""Data cleaning and feature engineering for OHLC analysis."""

from __future__ import annotations

import numpy as np
import pandas as pd


REQUIRED_OHLC = ("open", "high", "low", "close")


def ensure_utc_hourly_index(df_in: pd.DataFrame) -> tuple[pd.DataFrame, pd.DatetimeIndex]:
    """Ensure a UTC DatetimeIndex named ts_hour; report duplicates and gaps."""
    df_clean = df_in.copy()

    if not isinstance(df_clean.index, pd.DatetimeIndex) and "ts_hour" in df_clean.columns:
        df_clean = df_clean.set_index("ts_hour")

    if not isinstance(df_clean.index, pd.DatetimeIndex):
        coerced = pd.to_datetime(df_clean.index, errors="coerce", utc=True)
        nat_count = int(pd.isna(coerced).sum())
        if nat_count > 0:
            print(f"Dropping rows with non-parseable timestamps: {nat_count}")
        valid_mask = ~pd.isna(coerced)
        df_clean = df_clean.loc[valid_mask].copy()
        df_clean.index = pd.DatetimeIndex(coerced[valid_mask])

    if df_clean.index.tz is None:
        print("Index is timezone-naive. Localizing to UTC (no timezone conversion).")
        df_clean.index = df_clean.index.tz_localize("UTC")
    else:
        df_clean.index = df_clean.index.tz_convert("UTC")

    df_clean.index.name = "ts_hour"
    df_clean = df_clean.sort_index()

    dup_count = int(df_clean.index.duplicated(keep="first").sum())
    if dup_count > 0:
        print(f"Dropping duplicate timestamps (keeping first): {dup_count}")
        df_clean = df_clean[~df_clean.index.duplicated(keep="first")]
    else:
        print("No duplicate timestamps detected.")

    deltas = df_clean.index.to_series().diff().dropna()
    non_hourly = deltas[deltas != pd.Timedelta(hours=1)]
    print(f"Non-hourly index jumps detected: {len(non_hourly):,}")

    full_index = pd.date_range(
        start=df_clean.index.min(),
        end=df_clean.index.max(),
        freq="h",
        tz="UTC",
        name="ts_hour",
    )
    missing_index = full_index.difference(df_clean.index)
    print(f"Missing hourly timestamps: {len(missing_index):,}")
    if len(missing_index) > 0:
        gap_df = pd.DataFrame({"ts_hour": missing_index})
        gap_df["grp"] = (gap_df["ts_hour"].diff() != pd.Timedelta(hours=1)).cumsum()
        gap_ranges = gap_df.groupby("grp")["ts_hour"].agg(["min", "max", "count"]).head(5)
        print("Example missing-gap ranges (first 5):")
        print(gap_ranges)

    return df_clean, missing_index


def safe_log(x: pd.Series, name: str = "series") -> pd.Series:
    bad = x <= 0
    if np.any(bad):
        sample = x[bad].head(10)
        raise ValueError(f"Non-positive values found in {name}; cannot take log. Sample:\n{sample}")
    return np.log(x)


def validate_ohlc_dataframe(df_in: pd.DataFrame) -> pd.DataFrame:
    """Keep and validate OHLC only."""
    missing_cols = [c for c in REQUIRED_OHLC if c not in df_in.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df = df_in[list(REQUIRED_OHLC)].copy()

    for c in REQUIRED_OHLC:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    ohlc_missing_mask = df[list(REQUIRED_OHLC)].isna().any(axis=1)
    if ohlc_missing_mask.any():
        print(f"Dropping rows with missing OHLC values: {int(ohlc_missing_mask.sum())}")
        df = df.loc[~ohlc_missing_mask].copy()

    non_positive_mask = (df[list(REQUIRED_OHLC)] <= 0).any(axis=1)
    if non_positive_mask.any():
        bad_rows = df.loc[non_positive_mask, list(REQUIRED_OHLC)]
        raise ValueError(
            f"Found {non_positive_mask.sum()} rows with non-positive OHLC values. Sample:\n{bad_rows.head(10)}"
        )

    max_oc = df[["open", "close"]].max(axis=1)
    min_oc = df[["open", "close"]].min(axis=1)
    viol_high = df["high"] < max_oc
    viol_low = df["low"] > min_oc
    viol_hl = df["high"] < df["low"]
    viol_mask = viol_high | viol_low | viol_hl

    if viol_mask.any():
        print(f"OHLC consistency violations: {int(viol_mask.sum())}")
        print(df.loc[viol_mask, list(REQUIRED_OHLC)].head(10))
    else:
        print("No OHLC consistency violations detected.")

    return df


def build_feature_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Build analysis features without global dropna."""
    df_feat = df.copy()

    df_feat["log_close"] = safe_log(df_feat["close"], name="close")
    df_feat["log_open"] = safe_log(df_feat["open"], name="open")
    df_feat["log_high"] = safe_log(df_feat["high"], name="high")
    df_feat["log_low"] = safe_log(df_feat["low"], name="low")

    df_feat["log_return_close"] = df_feat["log_close"].diff()
    df_feat["log_return_open_to_close"] = df_feat["log_close"] - df_feat["log_open"]
    df_feat["log_return_close_to_open"] = df_feat["log_open"] - df_feat["log_close"].shift(1)

    df_feat["abs_r"] = df_feat["log_return_close"].abs()
    df_feat["sq_r"] = df_feat["log_return_close"] ** 2

    df_feat["hl_range"] = df_feat["log_high"] - df_feat["log_low"]
    df_feat["co_range"] = df_feat["log_close"] - df_feat["log_open"]
    df_feat["oc_range"] = df_feat["log_open"] - df_feat["log_close"].shift(1)

    df_feat["park_var"] = (1.0 / (4.0 * np.log(2.0))) * (df_feat["hl_range"] ** 2)
    df_feat["park_vol"] = np.sqrt(df_feat["park_var"])

    df_feat["rv_24h"] = df_feat["log_return_close"].rolling(24).std()
    df_feat["rv_72h"] = df_feat["log_return_close"].rolling(72).std()
    df_feat["rv_7d"] = df_feat["log_return_close"].rolling(24 * 7).std()
    df_feat["rv2_24h"] = df_feat["log_return_close"].rolling(24).var()
    df_feat["rv2_72h"] = df_feat["log_return_close"].rolling(72).var()
    df_feat["rv2_7d"] = df_feat["log_return_close"].rolling(24 * 7).var()

    df_feat["rolling_std_30d"] = df_feat["log_return_close"].rolling(24 * 30).std()
    df_feat["z_30d"] = df_feat["log_return_close"] / df_feat["rolling_std_30d"]

    df_feat["hour"] = df_feat.index.hour
    df_feat["day_of_week"] = df_feat.index.dayofweek
    df_feat["day_of_month"] = df_feat.index.day
    df_feat["month"] = df_feat.index.month
    df_feat["week_of_year"] = df_feat.index.isocalendar().week.astype(int)

    def cyc_features(k: pd.Series, period: int) -> tuple[pd.Series, pd.Series]:
        return np.sin(2.0 * np.pi * k / period), np.cos(2.0 * np.pi * k / period)

    df_feat["hour_sin"], df_feat["hour_cos"] = cyc_features(df_feat["hour"], 24)
    df_feat["dow_sin"], df_feat["dow_cos"] = cyc_features(df_feat["day_of_week"], 7)
    df_feat["month_sin"], df_feat["month_cos"] = cyc_features(df_feat["month"], 12)

    return df_feat
