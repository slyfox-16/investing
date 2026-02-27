"""Visualization helpers for OHLC time-series analysis."""

from __future__ import annotations

import numpy as np
import pandas as pd
import scipy.stats as st
from matplotlib import pyplot as plt
from statsmodels.graphics.gofplots import qqplot
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf


def plot_series(data: pd.Series | pd.DataFrame, title: str, ylabel: str) -> None:
    fig, ax = plt.subplots(figsize=(12, 4))
    data.plot(ax=ax)
    ax.set_title(title)
    ax.set_xlabel("ts_hour")
    ax.set_ylabel(ylabel)
    plt.tight_layout()
    plt.show()


def plot_visual_overview(df_a: pd.DataFrame) -> None:
    plot_series(df_a["close"], title="Asset Close Level", ylabel="Price")
    plot_series(df_a["log_return_close"], title="Hourly Log Return (Close-to-Close)", ylabel="Log return")
    plot_series(df_a["abs_r"], title="Absolute Log Returns", ylabel="Absolute log return")
    plot_series(df_a[["rv_24h", "rv_7d"]], title="Rolling Volatility: 24h vs 7d", ylabel="Std of log returns")
    plot_series(df_a["hl_range"], title="High-Low Log Range", ylabel="log(high/low)")


def plot_distribution_diagnostics(r_t: pd.Series, bins: int = 100) -> tuple[float, float, float]:
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.hist(r_t, bins=bins, alpha=0.8, edgecolor="black")
    ax.axvline(0.0, color="red", linestyle="--", linewidth=1.5)
    ax.set_title("Histogram of Hourly Log Returns")
    ax.set_xlabel("log return")
    ax.set_ylabel("Count")
    plt.tight_layout()
    plt.show()

    fig = plt.figure(figsize=(6, 6))
    qqplot(r_t, line="45", fit=True, ax=plt.gca())
    plt.title("QQ Plot: Returns vs Normal")
    plt.xlabel("Theoretical quantiles (Normal)")
    plt.ylabel("Sample quantiles")
    plt.tight_layout()
    plt.show()

    r_sorted = np.sort(r_t.values)
    n = len(r_sorted)
    probs = (np.arange(1, n + 1) - 0.5) / n
    t_df, t_loc, t_scale = st.t.fit(r_t.values)
    t_quant = st.t.ppf(probs, df=t_df, loc=t_loc, scale=t_scale)

    fig, ax = plt.subplots(figsize=(6, 6))
    ax.scatter(t_quant, r_sorted, s=8, alpha=0.6)
    line_min = min(t_quant.min(), r_sorted.min())
    line_max = max(t_quant.max(), r_sorted.max())
    ax.plot([line_min, line_max], [line_min, line_max], "r--", linewidth=1.5)
    ax.set_title("QQ Plot: Returns vs Fitted Student-t")
    ax.set_xlabel("Theoretical quantiles (fitted t)")
    ax.set_ylabel("Sample quantiles")
    plt.tight_layout()
    plt.show()

    print(f"Fitted Student-t params: df={t_df:.4f}, loc={t_loc:.6g}, scale={t_scale:.6g}")
    return float(t_df), float(t_loc), float(t_scale)


def plot_autocorrelation_diagnostics(r_t: pd.Series, abs_r: pd.Series, sq_r: pd.Series, lags: int = 96) -> None:
    fig, ax = plt.subplots(figsize=(12, 4))
    plot_acf(r_t, lags=lags, ax=ax, zero=False)
    ax.set_title("ACF of Returns (r_t)")
    ax.set_xlabel("Lag (hours)")
    ax.set_ylabel("Autocorrelation")
    plt.tight_layout()
    plt.show()

    fig, ax = plt.subplots(figsize=(12, 4))
    plot_pacf(r_t, lags=lags, ax=ax, method="ywm", zero=False)
    ax.set_title("PACF of Returns (r_t)")
    ax.set_xlabel("Lag (hours)")
    ax.set_ylabel("Partial autocorrelation")
    plt.tight_layout()
    plt.show()

    fig, ax = plt.subplots(figsize=(12, 4))
    plot_acf(abs_r, lags=lags, ax=ax, zero=False)
    ax.set_title("ACF of Absolute Returns |r_t|")
    ax.set_xlabel("Lag (hours)")
    ax.set_ylabel("Autocorrelation")
    plt.tight_layout()
    plt.show()

    fig, ax = plt.subplots(figsize=(12, 4))
    plot_acf(sq_r, lags=lags, ax=ax, zero=False)
    ax.set_title("ACF of Squared Returns r_t^2")
    ax.set_xlabel("Lag (hours)")
    ax.set_ylabel("Autocorrelation")
    plt.tight_layout()
    plt.show()


def plot_regime_volatility(df_a: pd.DataFrame, high_vol: pd.Series) -> None:
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(df_a.index, df_a["rv_7d"], label="rv_7d", linewidth=1.2)
    ax.fill_between(
        df_a.index,
        np.zeros(len(df_a)),
        df_a["rv_7d"].values,
        where=high_vol.values,
        color="red",
        alpha=0.20,
        label="High-vol regime (top quartile)",
    )
    ax.set_title("7-Day Rolling Volatility with High-Vol Regimes Highlighted")
    ax.set_xlabel("ts_hour")
    ax.set_ylabel("Rolling std")
    ax.legend()
    plt.tight_layout()
    plt.show()


def plot_seasonality(hour_stats: pd.DataFrame, dow_stats: pd.DataFrame) -> None:
    fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
    axes[0].plot(hour_stats.index, hour_stats["mean_return"], marker="o")
    axes[0].set_title("Mean Return by Hour")
    axes[0].set_ylabel("Mean return")
    axes[1].plot(hour_stats.index, hour_stats["std_return"], marker="o")
    axes[1].set_title("Return Std by Hour")
    axes[1].set_ylabel("Std return")
    axes[2].plot(hour_stats.index, hour_stats["extreme_freq"], marker="o")
    axes[2].set_title("Extreme Frequency P(|z_30d|>4) by Hour")
    axes[2].set_ylabel("Extreme freq")
    axes[2].set_xlabel("Hour of day")
    plt.tight_layout()
    plt.show()

    fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
    axes[0].plot(dow_stats.index, dow_stats["mean_return"], marker="o")
    axes[0].set_title("Mean Return by Day of Week")
    axes[0].set_ylabel("Mean return")
    axes[1].plot(dow_stats.index, dow_stats["std_return"], marker="o")
    axes[1].set_title("Return Std by Day of Week")
    axes[1].set_ylabel("Std return")
    axes[2].plot(dow_stats.index, dow_stats["extreme_freq"], marker="o")
    axes[2].set_title("Extreme Frequency P(|z_30d|>4) by Day of Week")
    axes[2].set_ylabel("Extreme freq")
    axes[2].set_xlabel("Day of week (0=Mon)")
    plt.tight_layout()
    plt.show()
