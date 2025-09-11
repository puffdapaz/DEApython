# dea_analyzer/diagnostics.py
"""
DEA diagnostics module:
- Normality tests (Shapiro-Wilk)
- KS tests comparing CRS vs VRS
"""

from scipy import stats


def run_diagnostics(efficiency_analysis):
    """
    Run diagnostics for DEA results (per year).

    Args:
        efficiency_analysis (dict): Dict of efficiency scores per year.

    Returns:
        dict: Diagnostics results.
    """
    diagnostics = {}

    for year, eff_scores in efficiency_analysis.items():
        year_diag = {}

        # Shapiro-Wilk test on VRS-input
        if "vrs_input" in eff_scores:
            year_diag["shapiro_vrs_input"] = stats.shapiro(eff_scores["vrs_input"])

        # KS test between CRS and VRS input
        if "crs_input" in eff_scores and "vrs_input" in eff_scores:
            year_diag["ks_crs_vs_vrs_input"] = stats.ks_2samp(
                eff_scores["crs_input"], eff_scores["vrs_input"]
            )

        diagnostics[year] = year_diag

    return diagnostics
