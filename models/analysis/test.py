"""Statistical tests used in OHLC analysis."""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import scipy.stats as st
import statsmodels.api as sm
from statsmodels.stats.diagnostic import acorr_ljungbox, het_arch
from statsmodels.stats.stattools import jarque_bera
from statsmodels.tsa.stattools import adfuller, kpss


def print_test_result(
    test_name: str,
    H0: str,
    H1: str,
    stat: float,
    pvalue: float,
    alpha: float = 0.05,
    extra: dict[str, object] | str | None = None,
    reject_meaning: str | None = None,
    fail_meaning: str | None = None,
) -> dict[str, object]:
    reject = bool(pvalue < alpha)
    if reject:
        meaning = reject_meaning or "Evidence supports H1."
        conclusion = f"Conclusion at alpha={alpha:.2f}: Reject H0 -> {meaning}"
    else:
        meaning = fail_meaning or "Insufficient evidence against H0."
        conclusion = f"Conclusion at alpha={alpha:.2f}: Fail to reject H0 -> {meaning}"

    print(f"TEST: {test_name}")
    print(f"H0: {H0}")
    print(f"H1: {H1}")
    print(f"Statistic: {stat:.6g}, p-value: {pvalue:.6g}")
    if extra is not None:
        if isinstance(extra, dict):
            for k, v in extra.items():
                print(f"{k}: {v}")
        else:
            print(extra)
    print(conclusion)
    print("-" * 90)

    return {
        "test_name": test_name,
        "H0": H0,
        "H1": H1,
        "stat": float(stat),
        "pvalue": float(pvalue),
        "reject_H0": reject,
        "conclusion": conclusion,
    }


def run_jarque_bera(r_t: pd.Series, alpha: float = 0.05) -> dict[str, object]:
    jb_stat, jb_p, jb_skew, jb_kurt = jarque_bera(r_t)
    return print_test_result(
        test_name="Jarque-Bera Normality Test (r_t)",
        H0="Returns are normally distributed (skew=0 and kurtosis=3).",
        H1="Returns are not normally distributed.",
        stat=jb_stat,
        pvalue=jb_p,
        alpha=alpha,
        extra={"sample_skew": float(jb_skew), "sample_kurtosis": float(jb_kurt)},
        reject_meaning="returns show non-normal shape (skew and/or heavy tails).",
        fail_meaning="normality is not strongly contradicted by this test.",
    )


def run_shapiro(r_t: pd.Series, alpha: float = 0.05, max_n: int = 5000) -> dict[str, object] | None:
    sample = r_t.dropna()
    if len(sample) < 3:
        return None
    if len(sample) > max_n:
        sample = sample.sample(max_n, random_state=42)
        print(f"Shapiro-Wilk note: using random subsample of {max_n:,} observations.")

    sh_stat, sh_p = st.shapiro(sample)
    return print_test_result(
        test_name="Shapiro-Wilk Normality Test (r_t)",
        H0="Returns follow a normal distribution.",
        H1="Returns do not follow a normal distribution.",
        stat=sh_stat,
        pvalue=sh_p,
        alpha=alpha,
        reject_meaning="distribution deviates from normality.",
        fail_meaning="normality is not strongly contradicted by this test.",
    )


def run_adf(series: pd.Series, series_name: str, alpha: float = 0.05) -> dict[str, object]:
    adf_stat, adf_p, used_lag, nobs, _crit_vals, icbest = adfuller(series, autolag="AIC")
    return print_test_result(
        test_name=f"ADF Test ({series_name})",
        H0="Series has a unit root (non-stationary).",
        H1="Series is stationary.",
        stat=adf_stat,
        pvalue=adf_p,
        alpha=alpha,
        extra={"used_lag": int(used_lag), "nobs": int(nobs), "icbest": float(icbest)},
        reject_meaning="series is likely stationary.",
        fail_meaning="unit-root non-stationarity remains plausible.",
    )


def run_kpss(series: pd.Series, series_name: str, regression: str, alpha: float = 0.05) -> dict[str, object]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        kpss_stat, kpss_p, n_lags, _crit_vals = kpss(series, regression=regression, nlags="auto")
    return print_test_result(
        test_name=f"KPSS Test ({series_name}, regression='{regression}')",
        H0="Series is stationary (around constant/trend per regression setting).",
        H1="Series is non-stationary.",
        stat=kpss_stat,
        pvalue=kpss_p,
        alpha=alpha,
        extra={"used_lags": int(n_lags)},
        reject_meaning="series shows evidence of non-stationarity.",
        fail_meaning="stationarity is plausible under the KPSS setup.",
    )


def run_ljung_box(
    series: pd.Series,
    lags: list[int],
    series_label: str,
    H0: str,
    H1: str,
    reject_meaning: str,
    fail_meaning: str,
    alpha: float = 0.05,
) -> pd.DataFrame:
    print(f"TEST FAMILY: Ljung-Box on {series_label}")
    print(f"H0: {H0}")
    print(f"H1: {H1}")
    rows: list[dict[str, object]] = []
    for lag in lags:
        lb = acorr_ljungbox(series, lags=[lag], return_df=True)
        stat = float(lb["lb_stat"].iloc[0])
        pvalue = float(lb["lb_pvalue"].iloc[0])
        reject = bool(pvalue < alpha)
        conclusion = (
            f"Conclusion at alpha={alpha:.2f}: Reject H0 -> {reject_meaning}"
            if reject
            else f"Conclusion at alpha={alpha:.2f}: Fail to reject H0 -> {fail_meaning}"
        )
        rows.append(
            {
                "lag": lag,
                "lb_stat": stat,
                "pvalue": pvalue,
                "reject_H0": reject,
                "conclusion": conclusion,
            }
        )
    out = pd.DataFrame(rows)
    print(out[["lag", "pvalue", "reject_H0"]].to_string(index=False))
    print(
        f"Overall summary: rejected at {int(out['reject_H0'].sum())}/{len(out)} lags. "
        "If many lags reject for squared returns, that supports conditional heteroskedasticity / GARCH."
    )
    print("-" * 90)
    return out


def run_arch_lm(r_t: pd.Series, alpha: float = 0.05, nlags: int = 24) -> dict[str, object]:
    arch_lm_stat, arch_lm_p, arch_f_stat, arch_f_p = het_arch(r_t, nlags=nlags)
    return print_test_result(
        test_name=f"Engle ARCH LM Test ({nlags} lags)",
        H0="No ARCH effects (homoskedastic variance).",
        H1="ARCH effects are present (time-varying variance).",
        stat=arch_lm_stat,
        pvalue=arch_lm_p,
        alpha=alpha,
        extra={"F_stat": float(arch_f_stat), "F_pvalue": float(arch_f_p)},
        reject_meaning="volatility is time-varying (ARCH effects present).",
        fail_meaning="no strong ARCH evidence at selected lags.",
    )


def run_asymmetry_regression(df_a: pd.DataFrame, alpha: float = 0.05) -> tuple[object, pd.DataFrame, dict[str, object]]:
    reg_df = df_a[["log_return_close", "sq_r", "abs_r"]].copy()
    reg_df["sq_r_l1"] = reg_df["sq_r"].shift(1)
    reg_df["neg_l1"] = (reg_df["log_return_close"].shift(1) < 0).astype(int)
    reg_df["asym_term"] = reg_df["neg_l1"] * reg_df["sq_r_l1"]
    reg_df = reg_df.dropna()

    X = sm.add_constant(reg_df[["sq_r_l1", "asym_term"]])
    y = reg_df["sq_r"]
    model = sm.OLS(y, X).fit(cov_type="HC3")

    coef = float(model.params["asym_term"])
    t_stat = float(model.tvalues["asym_term"])
    pvalue = float(model.pvalues["asym_term"])
    result = print_test_result(
        test_name="Leverage Asymmetry Term Test",
        H0="Asymmetry term coefficient = 0 (no leverage effect).",
        H1="Asymmetry term coefficient != 0 (leverage/asymmetry present).",
        stat=t_stat,
        pvalue=pvalue,
        alpha=alpha,
        extra={"asymmetry_coefficient": coef},
        reject_meaning="asymmetry/leverage effect is present.",
        fail_meaning="no strong evidence of leverage asymmetry in this regression.",
    )
    return model, model.summary2().tables[1], result


def run_anova(series: pd.Series, groups: pd.Series, label: str, alpha: float = 0.05) -> dict[str, object] | None:
    temp = pd.DataFrame({"y": series, "g": groups}).dropna()
    group_arrays = [g["y"].values for _, g in temp.groupby("g") if len(g) > 1]
    if len(group_arrays) < 2:
        return None
    f_stat, pvalue = st.f_oneway(*group_arrays)
    return print_test_result(
        test_name=f"ANOVA: Mean Returns by {label}",
        H0=f"Mean returns are equal across all {label} groups.",
        H1=f"At least one {label} group has a different mean return.",
        stat=f_stat,
        pvalue=pvalue,
        alpha=alpha,
        reject_meaning="mean returns vary across groups.",
        fail_meaning="no strong evidence that group means differ.",
    )
