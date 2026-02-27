"""Final model-design summary utilities."""

from __future__ import annotations

import numpy as np
import pandas as pd


def build_model_design_summary(
    test_results: dict[str, dict[str, object]],
    lb_r_df: pd.DataFrame,
    lb_sq_df: pd.DataFrame,
    regime_table: pd.DataFrame,
    hour_stats: pd.DataFrame,
    dow_stats: pd.DataFrame,
) -> str:
    mean_lb_reject_ratio = float(lb_r_df["reject_H0"].mean()) if not lb_r_df.empty else np.nan
    sq_lb_reject_ratio = float(lb_sq_df["reject_H0"].mean()) if not lb_sq_df.empty else np.nan
    arch_reject = bool(test_results.get("arch_lm", {}).get("reject_H0", False))
    asym_result = test_results.get("asymmetry_term", {})
    asym_reject = bool(asym_result.get("reject_H0", False))
    asym_stat = float(asym_result.get("stat", 0.0))
    jb_reject = bool(test_results.get("jarque_bera", {}).get("reject_H0", False))

    regime_high_ext = np.nan
    regime_low_ext = np.nan
    if not regime_table.empty and "regime" in regime_table.columns:
        rt = regime_table.set_index("regime")
        if "high_vol" in rt.index:
            regime_high_ext = float(rt.loc["high_vol", "P(|z_30d|>4)"])
        if "low_vol" in rt.index:
            regime_low_ext = float(rt.loc["low_vol", "P(|z_30d|>4)"])

    hour_vol_spread = float(hour_stats["std_return"].max() - hour_stats["std_return"].min())
    dow_vol_spread = float(dow_stats["std_return"].max() - dow_stats["std_return"].min())

    if mean_lb_reject_ratio <= 0.2:
        mean_msg = "Return autocorrelation appears weak across most lags; a near-zero mean process is plausible."
        mean_reco = "consider zero-mean innovations"
    else:
        mean_msg = "Return autocorrelation appears material at multiple lags; short-memory mean dynamics may help."
        mean_reco = "consider AR(1)/ARMA mean component"

    if sq_lb_reject_ratio >= 0.5 or arch_reject:
        vol_msg = "Squared-return autocorrelation and/or ARCH test indicates conditional heteroskedasticity."
    else:
        vol_msg = "Weak evidence of persistent variance dynamics in these diagnostics."

    if asym_reject and asym_stat > 0:
        asym_msg = "Leverage asymmetry term is significant and positive: negative returns amplify next-step variance."
        asym_reco = "prefer GJR-GARCH or EGARCH"
    elif asym_reject:
        asym_msg = "Asymmetry is significant but sign is non-standard; asymmetry modeling is still warranted."
        asym_reco = "test asymmetric variants (GJR/EGARCH) and compare fit"
    else:
        asym_msg = "No strong leverage asymmetry detected in the chosen regression specification."
        asym_reco = "symmetric GARCH may suffice initially"

    if jb_reject:
        tail_msg = "Normality is rejected; tails are heavier/skewed relative to Gaussian benchmark."
    else:
        tail_msg = "Normality not strongly rejected in this sample."

    if np.isfinite(regime_high_ext) and np.isfinite(regime_low_ext) and regime_high_ext > regime_low_ext:
        regime_msg = "High-vol regimes have higher extreme-move frequency, suggesting state-dependent tail/jump intensity."
    else:
        regime_msg = "Regime split does not show a clear increase in extremes for high-vol state."

    if (hour_vol_spread > 0) or (dow_vol_spread > 0):
        season_msg = "Volatility/extreme metrics vary by hour and/or day-of-week."
        season_reco = "include cyclical time features in variance/jump equations"
    else:
        season_msg = "Limited evidence of intraday/weekly volatility seasonality."
        season_reco = "time-seasonal terms may be optional"

    return f"""
- Mean dynamics:
  - {mean_msg}
  - Evidence source: Ljung-Box on returns (reject ratio={mean_lb_reject_ratio:.2f}).

- Volatility clustering:
  - {vol_msg}
  - Evidence source: Ljung-Box on squared returns (reject ratio={sq_lb_reject_ratio:.2f}) and ARCH LM.

- Asymmetry:
  - {asym_msg}
  - Evidence source: leverage regression asymmetry term.

- Heavy tails / jumps:
  - {tail_msg}
  - Evidence source: Jarque-Bera, QQ diagnostics, and extreme-frequency comparison.

- Regimes:
  - {regime_msg}
  - Evidence source: low/high `rv_7d` regime comparison.

- Seasonality:
  - {season_msg}
  - Evidence source: hour/day grouped volatility and extreme-frequency profiles.

- Recommendation template (filled from current diagnostics):
  - If return ACF is weak -> {mean_reco}; if persistent return ACF appears -> consider AR(1).
  - If ARCH present and asymmetry present -> {asym_reco}; otherwise start with symmetric volatility model.
  - If extreme frequency is heavy and regimes differ -> add jump component with state-dependent intensity.
  - If hour/DOW seasonality exists -> {season_reco}.
"""
