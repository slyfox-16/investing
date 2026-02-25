#!/usr/bin/env python3
"""Capture Deribit ETH option IV snapshots and build hourly surface features.

Example:
  python -m sources.deribit.eth_iv_surface \
    --raw-out data/deribit_eth_option_surface_raw.parquet \
    --hourly-out data/eth_iv_surface_hourly.parquet
"""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

import numpy as np
import pandas as pd

DEFAULT_BASE_URL = "https://www.deribit.com"
API_ENDPOINT_VERSION = "deribit_api_v2"
INSTRUMENT_RE = re.compile(r"^(?P<ccy>[A-Z]+)-(?P<day>\d{1,2})(?P<mon>[A-Z]{3})(?P<yy>\d{2})-(?P<strike>\d+(?:\.\d+)?)-(?P<cp>[CP])$")
MONTH_MAP = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch ETH option summaries from Deribit and build IV surface features.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Deribit API base URL.")
    parser.add_argument("--currency", default="ETH", help="Asset symbol (default ETH).")
    parser.add_argument("--timeout-sec", type=int, default=20, help="HTTP timeout in seconds.")
    parser.add_argument("--raw-out", default="data/deribit_eth_option_surface_raw.parquet", help="Raw snapshot parquet output path.")
    parser.add_argument("--hourly-out", default="data/eth_iv_surface_hourly.parquet", help="Hourly feature parquet output path.")
    parser.add_argument("--min-contracts", type=int, default=25, help="Minimum contracts to mark an hour as high quality.")
    parser.add_argument("--min-total-oi", type=float, default=50.0, help="Minimum total open interest to mark quality high.")
    parser.add_argument("--max-staleness-hours", type=int, default=24, help="Max forward-fill hours for missing/low quality rows.")
    return parser.parse_args()


def http_get_json(base_url: str, endpoint: str, params: dict[str, Any], timeout_sec: int) -> dict[str, Any]:
    query = urlencode(params)
    url = f"{base_url.rstrip('/')}/api/v2/{endpoint}?{query}"
    with urlopen(url, timeout=timeout_sec) as resp:  # noqa: S310
        return json.loads(resp.read().decode("utf-8"))


def parse_instrument_name(instrument_name: str) -> tuple[pd.Timestamp | None, float | None, str | None]:
    m = INSTRUMENT_RE.match(instrument_name)
    if not m:
        return None, None, None

    day = int(m.group("day"))
    month = MONTH_MAP[m.group("mon")]
    year = 2000 + int(m.group("yy"))
    strike = float(m.group("strike"))
    option_type = "call" if m.group("cp") == "C" else "put"

    # Deribit expiries are typically 08:00 UTC.
    expiry = pd.Timestamp(year=year, month=month, day=day, hour=8, tz="UTC")
    return expiry, strike, option_type


def parse_iv_value(value: Any) -> float | None:
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v) or v <= 0:
        return None
    if v > 5.0 and v <= 500.0:
        v = v / 100.0
    return v if 0 < v < 5 else None


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def fetch_snapshot(base_url: str, currency: str, timeout_sec: int) -> pd.DataFrame:
    snap_ts = pd.Timestamp.now(tz="UTC")

    instruments_res = http_get_json(
        base_url=base_url,
        endpoint="public/get_instruments",
        params={"currency": currency, "kind": "option", "expired": "false"},
        timeout_sec=timeout_sec,
    )
    summaries_res = http_get_json(
        base_url=base_url,
        endpoint="public/get_book_summary_by_currency",
        params={"currency": currency, "kind": "option"},
        timeout_sec=timeout_sec,
    )

    instruments = instruments_res.get("result", [])
    summaries = summaries_res.get("result", [])
    if not isinstance(instruments, list) or not isinstance(summaries, list):
        raise RuntimeError("Unexpected Deribit response shape for instruments/summaries")

    expiry_by_name: dict[str, pd.Timestamp] = {}
    strike_by_name: dict[str, float] = {}
    type_by_name: dict[str, str] = {}
    for ins in instruments:
        name = ins.get("instrument_name")
        if not name:
            continue

        expiry_ts = ins.get("expiration_timestamp")
        strike = ins.get("strike")
        option_type = ins.get("option_type")

        if expiry_ts is not None:
            expiry = pd.to_datetime(expiry_ts, unit="ms", utc=True)
        else:
            expiry, strike_parsed, option_type_parsed = parse_instrument_name(name)
            strike = strike if strike is not None else strike_parsed
            option_type = option_type if option_type is not None else option_type_parsed

        if expiry is not None:
            expiry_by_name[name] = expiry
        if strike is not None:
            strike_by_name[name] = float(strike)
        if option_type is not None:
            type_by_name[name] = str(option_type).lower()

    rows: list[dict[str, Any]] = []
    for s in summaries:
        name = s.get("instrument_name")
        if not name:
            continue
        if name not in expiry_by_name:
            expiry, strike, option_type = parse_instrument_name(name)
            if expiry is not None:
                expiry_by_name[name] = expiry
            if strike is not None:
                strike_by_name[name] = strike
            if option_type is not None:
                type_by_name[name] = option_type

        rows.append(
            {
                "snapshot_ts": snap_ts,
                "instrument_name": name,
                "expiry_ts": expiry_by_name.get(name),
                "strike": strike_by_name.get(name),
                "option_type": type_by_name.get(name),
                "mark_iv": parse_iv_value(s.get("mark_iv")),
                "underlying_price": to_float(s.get("underlying_price") or s.get("underlying_index_price") or s.get("index_price")),
                "open_interest": to_float(s.get("open_interest")),
                "bid_price": to_float(s.get("bid_price")),
                "ask_price": to_float(s.get("ask_price")),
                "mark_price": to_float(s.get("mark_price")),
                "source": "deribit_option_summary",
                "ingested_at": snap_ts,
                "api_endpoint_version": API_ENDPOINT_VERSION,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "snapshot_ts",
                "instrument_name",
                "expiry_ts",
                "strike",
                "option_type",
                "mark_iv",
                "underlying_price",
                "open_interest",
                "bid_price",
                "ask_price",
                "mark_price",
                "source",
                "ingested_at",
                "api_endpoint_version",
            ]
        )

    return df.drop_duplicates(subset=["snapshot_ts", "instrument_name"], keep="last")


def weighted_mean(series: pd.Series, weights: pd.Series) -> float | None:
    if series.empty:
        return None
    s = series.astype(float)
    w = weights.astype(float).clip(lower=0)
    mask = s.notna() & w.notna()
    if not mask.any():
        return None
    s = s[mask]
    w = w[mask]
    if w.sum() <= 0:
        return float(s.mean())
    return float(np.average(s, weights=w))


def interpolate_by_tenor(points: pd.DataFrame, target_days: float) -> float | None:
    if points.empty:
        return None
    points = points.sort_values("days_to_expiry")
    x = points["days_to_expiry"].to_numpy(dtype=float)
    y = points["atm_iv"].to_numpy(dtype=float)

    if len(points) == 1:
        return float(y[0])

    if target_days <= x[0]:
        return float(y[0])
    if target_days >= x[-1]:
        return float(y[-1])

    return float(np.interp(target_days, x, y))


def nearest_moneyness_iv(df: pd.DataFrame, target_m: float, option_type: str | None = None) -> float | None:
    sel = df
    if option_type is not None:
        sel = sel[sel["option_type"] == option_type]
    sel = sel.dropna(subset=["moneyness", "mark_iv"])
    if sel.empty:
        return None
    idx = (sel["moneyness"] - target_m).abs().idxmin()
    return float(sel.loc[idx, "mark_iv"])


def compute_hour_features(hour_df: pd.DataFrame, min_contracts: int, min_total_oi: float) -> dict[str, Any]:
    valid = hour_df.dropna(subset=["mark_iv", "underlying_price", "strike", "expiry_ts"]).copy()
    valid = valid[(valid["mark_iv"] > 0) & (valid["mark_iv"] < 5)]

    if valid.empty:
        return {
            "iv_atm_7d": None,
            "iv_atm_14d": None,
            "iv_atm_30d": None,
            "iv_atm_60d": None,
            "rr_25d_30d": None,
            "bf_25d_30d": None,
            "atm_term_slope_7d_30d": None,
            "atm_term_curvature_7d_30d_60d": None,
            "n_contracts_used": 0,
            "oi_weighted_iv_30d": None,
            "quality_flag": "insufficient_liquidity",
            "total_oi": 0.0,
        }

    valid["oi_w"] = valid["open_interest"].fillna(0.0).clip(lower=0.0)
    valid["days_to_expiry"] = (valid["expiry_ts"] - valid["ts_hour"]).dt.total_seconds() / 86400.0
    valid = valid[valid["days_to_expiry"] > 0]

    valid["moneyness"] = valid["strike"] / valid["underlying_price"]
    atm_slice = valid[valid["moneyness"].between(0.9, 1.1)]

    tenor_rows: list[dict[str, float]] = []
    for _, g in atm_slice.groupby("expiry_ts", sort=True):
        iv = weighted_mean(g["mark_iv"], g["oi_w"])
        tenor_rows.append(
            {
                "days_to_expiry": float(g["days_to_expiry"].median()),
                "atm_iv": float(g["mark_iv"].mean()) if iv is None else iv,
            }
        )
    tenor_points = pd.DataFrame(tenor_rows, columns=["days_to_expiry", "atm_iv"])

    iv_atm_7d = interpolate_by_tenor(tenor_points, 7.0)
    iv_atm_14d = interpolate_by_tenor(tenor_points, 14.0)
    iv_atm_30d = interpolate_by_tenor(tenor_points, 30.0)
    iv_atm_60d = interpolate_by_tenor(tenor_points, 60.0)

    around_30d = valid[(valid["days_to_expiry"] >= 23) & (valid["days_to_expiry"] <= 37)]
    call_25 = nearest_moneyness_iv(around_30d, target_m=1.1, option_type="call")
    put_25 = nearest_moneyness_iv(around_30d, target_m=0.9, option_type="put")
    atm_30 = nearest_moneyness_iv(around_30d, target_m=1.0, option_type=None)

    rr_25d_30d = None if call_25 is None or put_25 is None else call_25 - put_25
    bf_25d_30d = None if call_25 is None or put_25 is None or atm_30 is None else 0.5 * (call_25 + put_25) - atm_30

    slope_7_30 = None if iv_atm_7d is None or iv_atm_30d is None else (iv_atm_30d - iv_atm_7d) / (30.0 - 7.0)
    curv_7_30_60 = (
        None
        if iv_atm_7d is None or iv_atm_30d is None or iv_atm_60d is None
        else iv_atm_60d - 2 * iv_atm_30d + iv_atm_7d
    )

    oi_weighted_iv_30d = weighted_mean(around_30d["mark_iv"], around_30d["oi_w"])
    n_contracts = int(len(valid))
    total_oi = float(valid["oi_w"].sum())

    core_ok = iv_atm_7d is not None and iv_atm_30d is not None
    high_quality = n_contracts >= min_contracts and total_oi >= min_total_oi and core_ok

    return {
        "iv_atm_7d": iv_atm_7d,
        "iv_atm_14d": iv_atm_14d,
        "iv_atm_30d": iv_atm_30d,
        "iv_atm_60d": iv_atm_60d,
        "rr_25d_30d": rr_25d_30d,
        "bf_25d_30d": bf_25d_30d,
        "atm_term_slope_7d_30d": slope_7_30,
        "atm_term_curvature_7d_30d_60d": curv_7_30_60,
        "n_contracts_used": n_contracts,
        "oi_weighted_iv_30d": oi_weighted_iv_30d,
        "quality_flag": "ok" if high_quality else "insufficient_liquidity",
        "total_oi": total_oi,
    }


def build_hourly_features(
    df_raw: pd.DataFrame,
    min_contracts: int,
    min_total_oi: float,
    max_staleness_hours: int,
) -> pd.DataFrame:
    cols = [
        "ts_hour",
        "iv_atm_7d",
        "iv_atm_14d",
        "iv_atm_30d",
        "iv_atm_60d",
        "rr_25d_30d",
        "bf_25d_30d",
        "atm_term_slope_7d_30d",
        "atm_term_curvature_7d_30d_60d",
        "n_contracts_used",
        "oi_weighted_iv_30d",
        "source",
        "ingested_at",
        "api_endpoint_version",
        "quality_flag",
    ]

    if df_raw.empty:
        return pd.DataFrame(columns=cols)

    df = df_raw.copy()
    df["snapshot_ts"] = pd.to_datetime(df["snapshot_ts"], utc=True)
    df["expiry_ts"] = pd.to_datetime(df["expiry_ts"], utc=True)
    df["ts_hour"] = df["snapshot_ts"].dt.floor("h")

    per_hour_rows: list[dict[str, Any]] = []
    for ts_hour, grp in df.groupby("ts_hour", sort=True):
        feature_row = compute_hour_features(grp.assign(ts_hour=ts_hour), min_contracts=min_contracts, min_total_oi=min_total_oi)
        feature_row["ts_hour"] = ts_hour
        feature_row["source"] = "deribit_option_surface"
        feature_row["ingested_at"] = pd.Timestamp.now(tz="UTC")
        feature_row["api_endpoint_version"] = API_ENDPOINT_VERSION
        per_hour_rows.append(feature_row)

    hourly = pd.DataFrame(per_hour_rows).sort_values("ts_hour")

    full_index = pd.date_range(start=hourly["ts_hour"].min(), end=hourly["ts_hour"].max(), freq="1h", tz="UTC")
    hourly = hourly.set_index("ts_hour").reindex(full_index).rename_axis("ts_hour").reset_index()

    fill_cols = [
        "iv_atm_7d",
        "iv_atm_14d",
        "iv_atm_30d",
        "iv_atm_60d",
        "rr_25d_30d",
        "bf_25d_30d",
        "atm_term_slope_7d_30d",
        "atm_term_curvature_7d_30d_60d",
        "oi_weighted_iv_30d",
    ]
    for col in fill_cols:
        hourly[col] = hourly[col].ffill(limit=max_staleness_hours)

    hourly["n_contracts_used"] = hourly["n_contracts_used"].fillna(0).astype(int)
    hourly["source"] = hourly["source"].fillna("deribit_option_surface")
    hourly["ingested_at"] = hourly["ingested_at"].fillna(pd.Timestamp.now(tz="UTC"))
    hourly["api_endpoint_version"] = hourly["api_endpoint_version"].fillna(API_ENDPOINT_VERSION)

    original_quality = hourly["quality_flag"]
    filled_mask = original_quality.isna() & hourly[fill_cols].notna().any(axis=1)
    missing_mask = original_quality.isna() & ~filled_mask
    hourly.loc[filled_mask, "quality_flag"] = "stale_fill"
    hourly.loc[missing_mask, "quality_flag"] = "insufficient_liquidity"

    return hourly[cols]


def load_existing_raw(path: str) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except FileNotFoundError:
        return pd.DataFrame()


def run_deribit_eth_iv_surface(
    base_url: str = DEFAULT_BASE_URL,
    currency: str = "ETH",
    timeout_sec: int = 20,
    raw_out_path: str = "data/deribit_eth_option_surface_raw.parquet",
    hourly_out_path: str = "data/eth_iv_surface_hourly.parquet",
    min_contracts: int = 25,
    min_total_oi: float = 50.0,
    max_staleness_hours: int = 24,
) -> dict[str, str | int | float]:
    new_snapshot = fetch_snapshot(base_url=base_url, currency=currency, timeout_sec=timeout_sec)
    if new_snapshot.empty:
        raise SystemExit("No option summaries fetched from Deribit.")

    existing = load_existing_raw(raw_out_path)
    combined = pd.concat([existing, new_snapshot], ignore_index=True)
    combined = combined.drop_duplicates(subset=["snapshot_ts", "instrument_name"], keep="last")
    combined = combined.sort_values(["snapshot_ts", "instrument_name"]).reset_index(drop=True)

    hourly = build_hourly_features(
        df_raw=combined,
        min_contracts=min_contracts,
        min_total_oi=min_total_oi,
        max_staleness_hours=max_staleness_hours,
    )

    if hourly["ts_hour"].duplicated().any():
        raise SystemExit("Duplicate ts_hour detected in surface hourly output.")

    Path(raw_out_path).parent.mkdir(parents=True, exist_ok=True)
    Path(hourly_out_path).parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(raw_out_path, index=False)
    hourly.to_parquet(hourly_out_path, index=False)

    print(f"Wrote {len(combined)} rows to {raw_out_path}")
    print(f"Wrote {len(hourly)} rows to {hourly_out_path}")
    latest = hourly.iloc[-1]
    print(
        "Latest hour: "
        f"{latest['ts_hour']} iv_atm_30d={latest['iv_atm_30d']} "
        f"quality={latest['quality_flag']}"
    )
    return {
        "raw_rows": int(len(combined)),
        "hourly_rows": int(len(hourly)),
        "raw_out": raw_out_path,
        "hourly_out": hourly_out_path,
        "latest_quality_flag": str(latest["quality_flag"]),
    }


def main() -> int:
    args = parse_args()
    run_deribit_eth_iv_surface(
        base_url=args.base_url,
        currency=args.currency,
        timeout_sec=args.timeout_sec,
        raw_out_path=args.raw_out,
        hourly_out_path=args.hourly_out,
        min_contracts=args.min_contracts,
        min_total_oi=args.min_total_oi,
        max_staleness_hours=args.max_staleness_hours,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
