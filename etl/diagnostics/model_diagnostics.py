import os
import pandas as pd
import numpy as np
from scipy import stats
import logging
from typing import Dict, Any
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
    
    return bucket_name

# ------------------
# Descriptive Analysis
# ------------------

def analyze_silver_data(df):
    """Generate analysis of silver data."""
    if df is None:
        logger.warning("No DataFrame provided for analysis.")
        return
    print("Silver Data Analysis:")
    for year in sorted(df['ano'].unique()):
        year_data = df[df['ano'] == year]
        print(f"Year {year}: {len(year_data)} records")
        print("\n%s", year_data.describe().to_string())
        print("\n%s", year_data.corr(numeric_only=True).to_string())


def analyze_gold_data(gold_df):
    """Generate analysis of gold data."""
    if gold_df is None:
        logger.warning("No DataFrame provided for analysis.")
        return
    print("Gold Data Analysis:")
    for year in sorted(gold_df['ano'].unique()):
        year_data = gold_df[gold_df['ano'] == year]
        print(f"Year {year}: {len(year_data)} records")
        print("\n%s", year_data.describe().to_string())
        print("\n%s", year_data.corr(numeric_only=True).to_string())

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


def run_diagnostics(gold_df, *, log: bool = True):
    if gold_df is None:
        logger.warning("No gold_df provided to run_diagnostics")
        return {}

    gold_df = gold_df[gold_df["is_complete_grouped"] == True]

    diagnostics_tests = {}
    diagnostics_summary = []

    for year in sorted(gold_df['ano'].unique()):
        year_gold_df = gold_df[gold_df['ano'] == year]
        year_tests = {}

        # Statistical tests
        year_tests["shapiro_scale_eff"] = shapiro_wilk_function(
            year_gold_df["DEA_scale_efficiency"].to_numpy()
        )
        year_tests["ks_crs_vs_vrs"] = kolmogorov_smirnov_function(
            year_gold_df["DEA_crs_input"].to_numpy(),
            year_gold_df["DEA_vrs_input"].to_numpy()
        )
        year_tests["scale_eff"] = scale_efficiency_function(
            year_gold_df["DEA_scale_efficiency"].to_numpy()
        )
        year_tests["returns_to_scale"] = {
            "crs_vs_vrs": kolmogorov_smirnov_function(
                year_gold_df["DEA_crs_input"].to_numpy(),
                year_gold_df["DEA_vrs_input"].to_numpy()
            ),
            "irs_vs_drs": kolmogorov_smirnov_function(
                year_gold_df["DEA_irs_input"].to_numpy(),
                year_gold_df["DEA_drs_input"].to_numpy()
            ),
        }

        diagnostics_tests[year] = year_tests

        # Descriptive + correlation
        desc = year_gold_df.describe().reset_index()
        desc['ano'] = year  # add year for stacking all years
        diagnostics_summary.append(desc)

        corr = year_gold_df.corr(numeric_only=True).reset_index()
        corr['ano'] = year
        diagnostics_summary.append(corr)

        if log:
            log_test_results(f"Year {year}", year_tests)

    # Combine descriptive/corr into a single DataFrame
    diagnostics_summary_df = pd.concat(diagnostics_summary, ignore_index=True)

    # Save files locally & GCS
    bucket_name = setup_basedosdados()
    local_path = Path("data/processed/gold")
    local_path.mkdir(parents=True, exist_ok=True)

    # 1. Statistical tests → JSON
    save_dataframe(diagnostics_tests, "diagnostics_tests", directory=local_path, file_format="json")
    save_dataframe_to_gcs(diagnostics_tests, "diagnostics_tests", bucket_name, layer="gold", file_format="json")

    # 2. Describe + correlation → Parquet
    save_dataframe(diagnostics_summary_df, "diagnostics_summary", directory=local_path, file_format="parquet")
    save_dataframe_to_gcs(diagnostics_summary_df, "diagnostics_summary", bucket_name, layer="gold", file_format="parquet")

    return diagnostics_tests, diagnostics_summary_df


def log_test_results(test_name: str, results, indent: int = 0):
    """Nicely format and log a single test result (handles nested dicts)."""
    prefix = " " * indent
    print(f"{prefix}📊 {test_name} Results")

    if isinstance(results, dict):
        for k, v in results.items():
            if isinstance(v, dict):
                log_test_results(k, v, indent + 3)
            else:
                if isinstance(v, float):
                    print(f"{prefix}   {k}: {v:.4f}")
                else:
                    print(f"{prefix}   {k}: {v}")
    else:
        # handle strings or other single values
        print(f"{prefix}   {results}")

    if indent == 0:
        print("-" * 40)

