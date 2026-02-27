"""Generic OHLC time-series analysis entrypoint."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

try:
    from IPython.display import Markdown, display
except Exception:  # pragma: no cover - fallback for non-notebook environments
    def display(x: object) -> None:
        print(x)

    def Markdown(x: str) -> str:
        return x

from models.analysis.cleaning import (
    REQUIRED_OHLC,
    build_feature_frame,
    ensure_utc_hourly_index,
    validate_ohlc_dataframe,
)
from models.analysis.distribution import (
    compute_return_summary,
    extreme_vs_normal_table,
    hill_tail_table,
)
from models.analysis.summary import build_model_design_summary
from models.analysis.test import (
    print_test_result,
    run_adf,
    run_anova,
    run_arch_lm,
    run_asymmetry_regression,
    run_jarque_bera,
    run_kpss,
    run_ljung_box,
    run_shapiro,
)
from models.analysis.visuals import (
    plot_autocorrelation_diagnostics,
    plot_distribution_diagnostics,
    plot_regime_volatility,
    plot_seasonality,
    plot_visual_overview,
)
from models.analysis.load import load_ohlc_data


DEFAULT_LAGS = [1, 2, 3, 4, 5, 6, 8, 12, 16, 18, 24, 36, 48, 96]


def ohlc_time_series_analysis(
    asset: str,
    df: pd.DataFrame | None = None,
    config_path: str | Path | None = None,
    alpha: float = 0.05,
    lags: list[int] | None = None,
    run_plots: bool = True,
) -> dict[str, object]:
    """
    Run end-to-end OHLC time-series diagnostics for an asset.

    Returns a dictionary with cleaned data, features, diagnostics tables,
    and test results for downstream usage.
    """
    if lags is None:
        lags = DEFAULT_LAGS

    if df is None:
        df = load_ohlc_data(asset=asset, config_path=config_path)
        print(f"Loaded OHLC dataframe for asset={asset}.")

    df_indexed, missing_index = ensure_utc_hourly_index(df)
    df_ohlc = validate_ohlc_dataframe(df_indexed)

    print("Head:")
    display(df_ohlc.head())
    print("Tail:")
    display(df_ohlc.tail())
    print("Descriptive statistics:")
    display(df_ohlc.describe().T)

    df_a = build_feature_frame(df_ohlc)
    print(f"Final analysis shape: {df_a.shape}")
    print(f"Date range: {df_a.index.min()} -> {df_a.index.max()}")
    print("Non-null counts (selected columns):")
    display(
        df_a[
            [
                "open",
                "high",
                "low",
                "close",
                "log_return_close",
                "rv_7d",
                "z_30d",
            ]
        ]
        .notna()
        .sum()
        .to_frame("non_null_count")
    )

    if run_plots:
        plot_visual_overview(df_a)

    r_t = df_a["log_return_close"].dropna()
    abs_r = df_a["abs_r"].dropna()
    sq_r = df_a["sq_r"].dropna()

    summary_stats, percentiles, extreme_counts = compute_return_summary(r_t)
    print("Summary statistics for r_t:")
    display(summary_stats)
    print("Percentiles for r_t:")
    display(percentiles)
    print("Extreme-move counts:")
    display(extreme_counts)

    test_results: dict[str, dict[str, object]] = {}

    test_results["jarque_bera"] = run_jarque_bera(r_t, alpha=alpha)
    shapiro = run_shapiro(r_t, alpha=alpha)
    if shapiro is not None:
        test_results["shapiro"] = shapiro

    if run_plots:
        plot_distribution_diagnostics(r_t)

    log_close = df_a["log_close"].dropna()
    test_results["adf_log_close"] = run_adf(log_close, "log_close", alpha=alpha)
    test_results["kpss_log_close"] = run_kpss(log_close, "log_close", regression="ct", alpha=alpha)
    test_results["adf_r_t"] = run_adf(r_t, "log_return_close", alpha=alpha)
    test_results["kpss_r_t"] = run_kpss(r_t, "log_return_close", regression="c", alpha=alpha)
    print("Interpretation note: ADF and KPSS use opposite null hypotheses.")

    if run_plots:
        plot_autocorrelation_diagnostics(r_t, abs_r, sq_r, lags=max(lags))

    lb_r_df = run_ljung_box(
        series=r_t,
        lags=lags,
        series_label="r_t",
        H0="No autocorrelation up to lag L.",
        H1="Autocorrelation exists up to lag L.",
        reject_meaning="autocorrelation is present in returns.",
        fail_meaning="no strong evidence of return autocorrelation up to that lag.",
        alpha=alpha,
    )
    lb_sq_df = run_ljung_box(
        series=sq_r,
        lags=lags,
        series_label="r_t^2",
        H0="No autocorrelation in squared returns up to lag L (no volatility clustering).",
        H1="Autocorrelation exists in squared returns up to lag L (volatility clustering).",
        reject_meaning="volatility clustering is present.",
        fail_meaning="no strong evidence of volatility clustering up to that lag.",
        alpha=alpha,
    )

    test_results["arch_lm"] = run_arch_lm(r_t, alpha=alpha, nlags=24)

    asym_model, asym_coef_table, asym_result = run_asymmetry_regression(df_a, alpha=alpha)
    test_results["asymmetry_term"] = asym_result
    print("OLS (HC3 robust SE) coefficient table:")
    display(asym_coef_table)
    asym_coef = float(asym_model.params["asym_term"])
    asym_p = float(asym_model.pvalues["asym_term"])
    if asym_p < alpha and asym_coef > 0:
        print(
            "If asymmetry term > 0 and significant: negative returns increase future variance more -> "
            "GJR/EGARCH appropriate."
        )
    elif asym_p < alpha and asym_coef < 0:
        print("Asymmetry term is significant with opposite sign to standard leverage expectation.")
    else:
        print("No strong evidence of leverage asymmetry from this specification.")

    z = df_a["z_30d"].dropna()
    tail_compare_df = extreme_vs_normal_table(z)
    display(tail_compare_df)
    if (tail_compare_df["empirical_prob"] > tail_compare_df["normal_prob"]).all():
        print(
            "If empirical >> theoretical, tails are heavier than normal; "
            "consider Student-t innovations and/or jumps."
        )
    else:
        print("Heavy-tail evidence is mixed versus normal benchmark.")

    hill_df = hill_tail_table(r_t)
    display(hill_df)
    print("Interpretation: smaller Hill tail index implies heavier tails.")

    q75 = df_a["rv_7d"].quantile(0.75)
    q25 = df_a["rv_7d"].quantile(0.25)
    high_vol = df_a["rv_7d"] > q75
    low_vol = df_a["rv_7d"] < q25

    def regime_stats(mask: pd.Series, name: str) -> dict[str, object]:
        sub = df_a.loc[mask, "log_return_close"].dropna()
        z_sub = df_a.loc[mask, "z_30d"].dropna()
        return {
            "regime": name,
            "count": int(sub.shape[0]),
            "mean": float(sub.mean()) if len(sub) else None,
            "std": float(sub.std()) if len(sub) else None,
            "kurtosis_nonfisher": float(sub.kurt()) + 3.0 if len(sub) > 3 else None,
            "P(|z_30d|>4)": float((z_sub.abs() > 4).mean()) if len(z_sub) else None,
        }

    regime_table = pd.DataFrame([regime_stats(low_vol, "low_vol"), regime_stats(high_vol, "high_vol")])
    display(regime_table)
    print("If high-vol regime has heavier tails/more extremes, jump intensity may be state-dependent.")
    if run_plots:
        plot_regime_volatility(df_a, high_vol)

    def extreme_freq(series: pd.Series, threshold: int = 4) -> float:
        s = series.dropna()
        return float((s.abs() > threshold).mean()) if len(s) else float("nan")

    hour_stats = df_a.groupby("hour").agg(
        mean_return=("log_return_close", "mean"),
        std_return=("log_return_close", "std"),
        mean_abs_return=("abs_r", "mean"),
        extreme_freq=("z_30d", lambda s: extreme_freq(s, threshold=4)),
    )
    dow_stats = df_a.groupby("day_of_week").agg(
        mean_return=("log_return_close", "mean"),
        std_return=("log_return_close", "std"),
        mean_abs_return=("abs_r", "mean"),
        extreme_freq=("z_30d", lambda s: extreme_freq(s, threshold=4)),
    )
    print("Hour-of-day stats:")
    display(hour_stats)
    print("Day-of-week stats (0=Mon):")
    display(dow_stats)
    if run_plots:
        plot_seasonality(hour_stats, dow_stats)

    anova_hour = run_anova(df_a["log_return_close"], df_a["hour"], label="hour", alpha=alpha)
    if anova_hour is not None:
        test_results["anova_hour_mean"] = anova_hour
    anova_dow = run_anova(df_a["log_return_close"], df_a["day_of_week"], label="day_of_week", alpha=alpha)
    if anova_dow is not None:
        test_results["anova_dow_mean"] = anova_dow

    summary_md = build_model_design_summary(
        test_results=test_results,
        lb_r_df=lb_r_df,
        lb_sq_df=lb_sq_df,
        regime_table=regime_table,
        hour_stats=hour_stats,
        dow_stats=dow_stats,
    )
    display(Markdown(summary_md))

    return {
        "df_ohlc": df_ohlc,
        "df_features": df_a,
        "missing_index": missing_index,
        "summary_stats": summary_stats,
        "percentiles": percentiles,
        "extreme_counts": extreme_counts,
        "test_results": test_results,
        "lb_r_df": lb_r_df,
        "lb_sq_df": lb_sq_df,
        "tail_compare_df": tail_compare_df,
        "hill_df": hill_df,
        "regime_table": regime_table,
        "hour_stats": hour_stats,
        "dow_stats": dow_stats,
        "summary_markdown": summary_md,
        "alpha": alpha,
        "lags": lags,
        "required_ohlc": REQUIRED_OHLC,
    }


__all__ = ["ohlc_time_series_analysis", "DEFAULT_LAGS", "print_test_result"]
