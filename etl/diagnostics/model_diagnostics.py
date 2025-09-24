import os
import numpy as np
from scipy import stats
import logging
from typing import Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv
from ..save_utils import save_dataframe, save_dataframe_to_gcs

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_basedosdados() -> str:
    """Set up Base dos Dados configuration and return bucket name."""
    load_dotenv()
    billing_project_id = os.getenv("billing_project_id")
    bucket_name = os.getenv("gcp_bucket_name")
    
    if not billing_project_id or not bucket_name:
        raise ValueError("Missing required environment variables")
    
    logger.info("Base dos Dados configured successfully")
    
    return bucket_name

# ------------------
# Config & Analysis
# ------------------

def analyze_silver_data(df):
    """Generate analysis of silver data."""
    if df is None:
        logger.warning("No DataFrame provided for analysis.")
        return
    logger.info("🔎 Silver Data Analysis:")
    for year in sorted(df['ano'].unique()):
        year_data = df[df['ano'] == year]
        logger.info(f"Year {year}: {len(year_data)} records")

def analyze_gold_data(gold_df):
    """Generate analysis of gold data."""
    if gold_df is None:
        logger.warning("No DataFrame provided for analysis.")
        return
    logger.info("🔎 Gold Data Analysis:")
    for year in sorted(gold_df['ano'].unique()):
        year_data = gold_df[gold_df['ano'] == year]
        logger.info(f"Year {year}: {len(year_data)} records")



# ------------------
# Statistical Tests
# ------------------

def shapiro_wilk_function(efficiency_scores: np.ndarray, alpha: float = 0.05) -> Dict[str, Any]:
    """Shapiro-Wilk test for normality. Returns dict (unchanged format)."""
    try:
        stat, p_value = stats.shapiro(efficiency_scores)
        is_normal = p_value > alpha
        return {
            "test": "Shapiro-Wilk",
            "statistic": float(stat),
            "p_value": float(p_value),
            "is_normal": bool(is_normal),
            "alpha": alpha,
            "interpretation": "Data is normally distributed" if is_normal else "Data is not normally distributed",
        }
    except Exception as e:
        logger.error(f"Shapiro-Wilk test failed: {e}")
        return {"error": str(e)}


def kolmogorov_smirnov_function(sample1: np.ndarray, sample2: np.ndarray, alpha: float = 0.05) -> Dict[str, Any]:
    """Two-sample KS test. Returns dict (unchanged format)."""
    try:
        stat, p_value = stats.ks_2samp(sample1, sample2)
        different_distributions = p_value < alpha
        return {
            "test": "Kolmogorov-Smirnov",
            "statistic": float(stat),
            "p_value": float(p_value),
            "different_distributions": bool(different_distributions),
            "alpha": alpha,
            "interpretation": "Samples from different distributions" if different_distributions else "Samples from same distribution",
        }
    except Exception as e:
        logger.error(f"KS test failed: {e}")
        return {"error": str(e)}


def scale_efficiency_function(scale_efficiencies: np.ndarray, alpha: float = 0.05) -> Dict[str, Any]:
    """One-sample t-test against mean=1 (unchanged format)."""
    try:
        t_stat, p_value = stats.ttest_1samp(scale_efficiencies, 1.0)
        significantly_different = p_value < alpha
        return {
            "test": "One-sample t-test (mean=1)",
            "t_statistic": float(t_stat),
            "p_value": float(p_value),
            "significantly_different": bool(significantly_different),
            "alpha": alpha,
            "mean_scale_efficiency": float(np.mean(scale_efficiencies)),
            "interpretation": "Scale efficiency significantly different from 1" if significantly_different else "No significant difference from perfect scale efficiency",
        }
    except Exception as e:
        logger.error(f"Scale efficiency test failed: {e}")
        return {"error": str(e)}


# -----------------------
# Per-year wrapper helpers
# -----------------------

def _safe_array(series) -> Optional[np.ndarray]:
    """Return a numpy array or None if insufficient data."""
    if series is None:
        return None
    arr = np.asarray(series.dropna())
    if arr.size < 3:
        # Shapiro requires n>=3; many tests also need some data
        return None
    return arr


def shapiro_wilk_test(gold_df, column: str, alpha: float = 0.05) -> Dict[int, Dict[str, Any]]:
    """
    Run Shapiro-Wilk per year for column. Returns { year: {column: shapiro_result_dict} }.
    """
    results: Dict[int, Dict[str, Any]] = {}
    gold_df = gold_df[gold_df["is_complete_grouped"] == True]
    for year in sorted(gold_df['ano'].unique()):
        year_gold_df = gold_df[gold_df['ano'] == year]
        arr = _safe_array(year_gold_df[column])  # returns None if <3 usable rows
        if arr is None:
            results[year] = {column: {"error": "insufficient data (need >=3 non-null)"}}
        else:
            results[year] = {column: shapiro_wilk_function(arr, alpha)}
    return results


def kolmogorov_smirnov_test(gold_df, col1: str, col2: str, alpha: float = 0.05) -> Dict[int, Dict[str, Any]]:
    """
    Run KS test per year for two columns. Returns { year: {"col1_vs_col2": ks_result_dict} }.
    """
    results: Dict[int, Dict[str, Any]] = {}
    gold_df = gold_df[gold_df["is_complete_grouped"] == True]
    for year in sorted(gold_df['ano'].unique()):
        year_gold_df = gold_df[gold_df['ano'] == year]
        a = _safe_array(year_gold_df[col1])
        b = _safe_array(year_gold_df[col2])
        if a is None or b is None:
            results[year] = {f"{col1}_vs_{col2}": {"error": "insufficient data (need >=3 non-null in each sample)"}}
        else:
            results[year] = {f"{col1}_vs_{col2}": kolmogorov_smirnov_function(a, b, alpha)}
    return results


def scale_efficiency_test(gold_df, column: str = "DEA_scale_efficiency", alpha: float = 0.05) -> Dict[int, Dict[str, Any]]:
    """
    Run scale_efficiency_test per year. Returns { year: {column: ttest_result_dict} }.
    """
    results: Dict[int, Dict[str, Any]] = {}
    gold_df = gold_df[gold_df["is_complete_grouped"] == True]
    for year in sorted(gold_df['ano'].unique()):
        year_gold_df = gold_df[gold_df['ano'] == year]
        arr = _safe_array(year_gold_df[column])
        if arr is None:
            results[year] = {column: {"error": "insufficient data (need >=3 non-null)"}}
        else:
            results[year] = {column: scale_efficiency_function(arr, alpha)}
    return results


def returns_to_scale_test(gold_df, crs_col: str = "DEA_crs_input", vrs_col: str = "DEA_vrs_input",
                             drs_col: str = "DEA_drs_input", irs_col: str = "DEA_irs_input",
                             alpha: float = 0.05) -> Dict[int, Dict[str, Any]]:
    """
    Run returns-to-scale comparisons per year (CRS vs VRS, IRS vs DRS).
    Returns { year: {"crs_vs_vrs": {...}, "irs_vs_drs": {...}} }.
    """
    results: Dict[int, Dict[str, Any]] = {}
    gold_df = gold_df[gold_df["is_complete_grouped"] == True]
    for year in sorted(gold_df['ano'].unique()):
        year_gold_df = gold_df[gold_df['ano'] == year]
        a = _safe_array(year_gold_df[crs_col])
        b = _safe_array(year_gold_df[vrs_col])
        c = _safe_array(year_gold_df[drs_col])
        d = _safe_array(year_gold_df[irs_col])
        year_res: Dict[str, Any] = {}
        if a is None or b is None:
            year_res["crs_vs_vrs"] = {"error": "insufficient data for crs/vrs (need >=3 non-null)"} 
        else:
            year_res["crs_vs_vrs"] = kolmogorov_smirnov_function(a, b, alpha)
        if c is None or d is None:
            year_res["irs_vs_drs"] = {"error": "insufficient data for irs/drs (need >=3 non-null)"}
        else:
            year_res["irs_vs_drs"] = kolmogorov_smirnov_function(d, c, alpha)  # note: order optional
        results[year] = year_res
    return results


# -------------
# Convenience runner
# -------------

def run_diagnostics(gold_df, *, log: bool = True) -> Dict[str, Dict[int, Dict[str, Any]]]:
    """
    Run a standard set of diagnostics per year and return a nested dict:
    If log=True, also send nicely-formatted output to logger using log_test_results.
    """
    if gold_df is None:
        logger.warning("No gold_df provided to run_diagnostics")
        return {}

    diagnostics: Dict[str, Dict[int, Dict[str, Any]]] = {}

    # Shapiro on scale efficiency
    diagnostics["shapiro_scale_eff"] = shapiro_wilk_test(gold_df, "DEA_scale_efficiency")

    # KS CRS vs VRS
    diagnostics["ks_crs_vs_vrs"] = kolmogorov_smirnov_test(gold_df, "DEA_crs_input", "DEA_vrs_input")

    # Scale efficiency t-test per year
    diagnostics["scale_eff"] = scale_efficiency_test(gold_df, "DEA_scale_efficiency")

    # Returns to scale (CRS vs VRS and IRS vs DRS)
    diagnostics["returns_to_scale"] = returns_to_scale_test(gold_df)

    if log:
        # use your existing logger helper to print nested results
        for diag_name, diag_data in diagnostics.items():
            for year, res in diag_data.items():
                # `res` is a dict of {metric: result dict}
                log_test_results(f"{diag_name} - Year {year}", res)

    # Save statistic tests file
    # Set up Base dos Dados
    bucket_name = setup_basedosdados()
    local_path = Path("data/processed/gold")
    local_path.mkdir(parents=True, exist_ok=True)
    
    save_dataframe(diagnostics, "diagnostics_summary", directory="data/processed/gold", file_format="json")
    save_dataframe_to_gcs(gold_df, "diagnostics_summary", bucket_name, layer="gold")

    return diagnostics


def log_test_results(test_name: str, results, indent: int = 0):
    """Nicely format and log a single test result (handles nested dicts)."""
    prefix = " " * indent
    logger.info(f"{prefix}📊 {test_name} Results")

    if isinstance(results, dict):
        for k, v in results.items():
            if isinstance(v, dict):
                log_test_results(k, v, indent + 3)
            else:
                if isinstance(v, float):
                    logger.info(f"{prefix}   {k}: {v:.4f}")
                else:
                    logger.info(f"{prefix}   {k}: {v}")
    else:
        # handle strings or other single values
        logger.info(f"{prefix}   {results}")

    if indent == 0:
        logger.info("-" * 40)

