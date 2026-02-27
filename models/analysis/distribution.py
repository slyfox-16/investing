"""Distribution and tail diagnostics helpers."""

from __future__ import annotations

import numpy as np
import pandas as pd
import scipy.stats as st


def compute_return_summary(r_t: pd.Series) -> tuple[pd.Series, pd.Series, pd.DataFrame]:
    summary = pd.Series(
        {
            "count": r_t.count(),
            "mean": r_t.mean(),
            "std": r_t.std(),
            "skew": r_t.skew(),
            "kurtosis_nonfisher": st.kurtosis(r_t, fisher=False, bias=False),
        },
        name="r_t",
    )

    percentiles = pd.Series(
        {
            "p0.1": r_t.quantile(0.001),
            "p1": r_t.quantile(0.01),
            "p5": r_t.quantile(0.05),
            "p50": r_t.quantile(0.50),
            "p95": r_t.quantile(0.95),
            "p99": r_t.quantile(0.99),
            "p99.9": r_t.quantile(0.999),
        },
        name="r_t",
    )

    std_r = r_t.std()
    rows = []
    for k in [2, 3, 4, 5]:
        mask = r_t.abs() > (k * std_r)
        rows.append(
            {
                "threshold": f"|r_t| > {k}*std",
                "count": int(mask.sum()),
                "frequency": float(mask.mean()),
            }
        )
    extreme_counts = pd.DataFrame(rows)
    return summary, percentiles, extreme_counts


def extreme_vs_normal_table(z: pd.Series, thresholds: tuple[int, ...] = (3, 4, 5)) -> pd.DataFrame:
    rows = []
    z_abs = z.dropna().abs()
    for thr in thresholds:
        empirical = float((z_abs > thr).mean())
        normal = float(2.0 * st.norm.sf(thr))
        rows.append(
            {
                "threshold": f"|z_30d| > {thr}",
                "empirical_prob": empirical,
                "normal_prob": normal,
                "empirical_to_normal_ratio": (empirical / normal) if normal > 0 else np.nan,
            }
        )
    return pd.DataFrame(rows)


def hill_estimator_tail(x: np.ndarray, k: int) -> float:
    arr = np.asarray(x, dtype=float)
    arr = arr[np.isfinite(arr)]
    arr = arr[arr > 0]
    n = len(arr)
    if n <= k + 1:
        return np.nan

    arr_sorted = np.sort(arr)
    x_kplus1 = arr_sorted[-k - 1]
    top_k = arr_sorted[-k:]

    if x_kplus1 <= 0:
        return np.nan

    logs = np.log(top_k / x_kplus1)
    denom = logs.sum()
    if denom <= 0:
        return np.nan
    return float(k / denom)


def hill_tail_table(r_t: pd.Series, k_candidates: tuple[int, ...] = (200, 500, 1000)) -> pd.DataFrame:
    upper_tail = r_t[r_t > 0].values
    lower_tail = (-r_t[r_t < 0]).values

    rows = []
    for k in k_candidates:
        rows.append(
            {
                "k": k,
                "upper_tail_index_alpha": hill_estimator_tail(upper_tail, k),
                "lower_tail_index_alpha": hill_estimator_tail(lower_tail, k),
            }
        )
    return pd.DataFrame(rows)
