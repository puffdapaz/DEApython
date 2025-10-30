"""
Statistical analysis and diagnostics for Data Envelopment Analysis (DEA) results.

This module provides:
- Descriptive statistics and correlation analysis
- Statistical tests for normality and distribution comparisons  
- Diagnostic reporting for DEA efficiency scores
- Results persistence to local and cloud storage
"""
import os
import logging
import pandas as pd
import numpy as np
import yaml
from scipy import stats
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv
from ..save_utils import save_data, save_data_to_gcs

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Config Loading
# ---------------------------------------------------------------------
def load_configs(config_path: str = "configs/path.yml") -> dict:
    """
    Load YAML configuration for paths and layers.
    Args:
        config_path: Path to the YAML configuration file
    Returns:
        Dict containing configuration parameters
    Raises:
        Exception: For other unexpected errors
    """
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error loading configs: {e}")
        raise

def setup_gcp_bd() -> str:
    """
    Configure GCP credentials and retrieve bucket name from environment variables.
    Returns:
        str: GCP bucket name for data storage
    Raises:
        ValueError: If required environment variables are missing
    """
    load_dotenv()
    billing_project_id = os.getenv("billing_project_id")
    bucket_name = os.getenv("gcp_bucket_name")
    if not billing_project_id or not bucket_name:
        raise ValueError("Missing required environment variables")
    return bucket_name

# ---------------------------------------------------------------------
# Descriptive Analysis
# ---------------------------------------------------------------------
def analyze_data(df: pd.DataFrame,
                 name: str) -> None:
    """
    Perform comprehensive descriptive analysis on a DataFrame.
    Generates summary statistics and correlation matrices for each year
    in the dataset and prints formatted results.
    Args:
        df: DataFrame to analyze
        name: Name of the dataset for reporting
    Raises:
        Exception: For other unexpected errors
    """
    try:
        if df is None:
            logger.warning(f"No DataFrame provided for {name} analysis.")
            return
        print(f"{name} data analysis:")
        for year in sorted(df['year'].unique()):
            year_data = df[df['year'] == year]
            print(year, f"{name} records:", len(year_data))
            print("\n%s", year_data.describe().to_string())
            print("\n%s", year_data.corr(numeric_only=True).to_string())
    except Exception as e:
        logger.error(f"Error analyzing data: {e}")
        raise
# ---------------------------------------------------------------------
# Statistical Tests
# ---------------------------------------------------------------------
def shapiro_wilk_test(efficiency_scores: np.ndarray,
                      alpha: float = 0.05) -> Dict[str, Any]:
    """
    Perform Shapiro-Wilk test for normality on efficiency scores.
    Args:
        efficiency_scores: Array of efficiency scores to test
        alpha: Significance level for hypothesis test
    Returns:
        Dict containing test results with keys:
        - test: Test name
        - statistic: Test statistic
        - p_value: P-value
        - alpha: Significance level used
        - reject_null: Boolean indicating if null hypothesis is rejected
        - interpretation: Human-readable interpretation of results
    Raises:
        Exception: For other unexpected errors
    """
    try:
        stat, p_value = stats.shapiro(efficiency_scores)
        reject_null = p_value < alpha
        return {
            "test": "Shapiro-Wilk",
            "statistic": float(stat),
            "p_value": float(p_value),
            "alpha": alpha,
            "reject_null": reject_null,
            "interpretation": "Reject H0 → not normal" if reject_null else "Fail to reject H0 → normal",
        }
    except Exception as e:
        logger.error(f"Shapiro-Wilk test failed: {e}")
        return {"error": str(e)}

def kolmogorov_smirnov_test(sample1: np.ndarray,
                            sample2: np.ndarray,
                            alpha: float = 0.05) -> Dict[str, Any]:
    """
    Perform two-sample Kolmogorov-Smirnov test for distribution comparison.
    Args:
        sample1: First sample array
        sample2: Second sample array  
        alpha: Significance level for hypothesis test
    Returns:
        Dict containing test results with keys:
        - test: Test name
        - statistic: Test statistic
        - p_value: P-value
        - alpha: Significance level used
        - reject_null: Boolean indicating if null hypothesis is rejected
        - interpretation: Human-readable interpretation of results
    Raises:
        Exception: For other unexpected errors 
    """
    try:
        stat, p_value = stats.ks_2samp(sample1, sample2)
        reject_null = p_value < alpha
        return {
            "test": "Kolmogorov-Smirnov",
            "statistic": float(stat),
            "p_value": float(p_value),
            "alpha": alpha,
            "reject_null": reject_null,
            "interpretation": "Reject H0 → distributions differ" if reject_null else "Fail to reject H0 → distributions similar",
        }
    except Exception as e:
        logger.error(f"KS test failed: {e}")
        return {"error": str(e)}

def scale_efficiency_test(scale_efficiencies: np.ndarray,
                          alpha: float = 0.05) -> Dict[str, Any]:
    """
    Perform one-sample t-test to check if scale efficiency mean equals 1.
    Args:
        scale_efficiencies: Array of scale efficiency scores
        alpha: Significance level for hypothesis test
    Returns:
        Dict containing test results with keys:
        - test: Test name
        - statistic: Test statistic
        - p_value: P-value
        - alpha: Significance level used
        - reject_null: Boolean indicating if null hypothesis is rejected
        - mean_scale_efficiency: Mean of scale efficiencies
        - interpretation: Human-readable interpretation of results
    Raises:
        Exception: For other unexpected errors
    """
    try:
        t_stat, p_value = stats.ttest_1samp(scale_efficiencies, 1.0)
        reject_null = p_value < alpha
        return {
            "test": "One-sample t-test (mean=1)",
            "statistic": float(t_stat),
            "p_value": float(p_value),
            "alpha": alpha,
            "reject_null": reject_null,
            "mean_scale_efficiency": float(np.mean(scale_efficiencies)),
            "interpretation": "Reject H0 → mean ≠ 1" if reject_null else "Fail to reject H0 → mean = 1",
        }
    except Exception as e:
        logger.error(f"Scale efficiency test failed: {e}")
        return {"error": str(e)}

# ---------------------------------------------------------------------
# Logging Utility
# ---------------------------------------------------------------------
def log_test_results(test_name: str,
                     results: dict[str, Any] | Any,
                     indent: int = 0) -> None:
    """
    Recursively format and log statistical test results in a readable tree structure.
    Args:
        test_name: Name of the test or result section
        results: Dictionary containing test results (can be nested)
        indent: Current indentation level for formatting.
    Returns:
        Formatted string representation of results.
    """
    prefix = " " * indent
    print(f"{prefix} {test_name} Results")
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
        print(f"{prefix}   {results}")
    if indent == 0:
        print("-" * 40)

# ---------------------------------------------------------------------
# Diagnostics Runner
# ---------------------------------------------------------------------
def run_diagnostics(gold_df: pd.DataFrame,
                    *,
                    log: bool = True,
                    alpha: float = 0.05) -> tuple[dict, pd.DataFrame]:
    """
    Statistical diagnostics on gold-level DEA results.
    Performs normality tests, distribution comparisons, and generates
    summary statistics for each year in the dataset.
    Args:
        gold_df: Gold-level DataFrame with DEA results
        log_results: Whether to log test results to console
        alpha: Significance level for statistical tests
    Returns:
    tuple[dict[str, dict[str, Any]], pd.DataFrame]:
        diagnostics_tests: Nested dictionary of statistical results by year.
        diagnostics_summary: Combined DataFrame of descriptive and correlation statistics.
    Raises:
        Exception: For other unexpected errors
    """
    try:
        if gold_df is None:
            logger.warning("No gold_df provided to run_diagnostics")
            return {}
        gold_df = gold_df[gold_df["is_complete_grouped"] == True]
        diagnostics_tests = {}
        diagnostics_summary = []
        for year in sorted(gold_df['year'].unique()):
            year_gold_df = gold_df[gold_df['year'] == year]
            year_tests = {}

            # Statistical tests
            year_tests["shapiro_scale_eff"] = shapiro_wilk_test(
                year_gold_df["DEA_scale_efficiency"].to_numpy(),
                alpha=alpha)
            year_tests["scale_eff"] = scale_efficiency_test(
                year_gold_df["DEA_scale_efficiency"].to_numpy(),
                alpha=alpha)
            year_tests["returns_to_scale"] = {
                "crs_vs_vrs": kolmogorov_smirnov_test(
                    year_gold_df["DEA_crs_input"].to_numpy(),
                    year_gold_df["DEA_vrs_input"].to_numpy(),
                    alpha=alpha),
                "irs_vs_drs": kolmogorov_smirnov_test(
                    year_gold_df["DEA_irs_input"].to_numpy(),
                    year_gold_df["DEA_drs_input"].to_numpy(),
                    alpha=alpha),
            }
            diagnostics_tests[year] = year_tests

            # Append descriptive + correlation
            desc = year_gold_df.describe().reset_index()
            desc['year'] = year 
            diagnostics_summary.append(desc)
            corr = year_gold_df.corr(numeric_only=True).reset_index()
            corr['year'] = year
            diagnostics_summary.append(corr)
            if log:
                log_test_results(f"Year {year}", year_tests)
        diagnostics_summary_df = pd.concat(diagnostics_summary, ignore_index=True)

        # Save outputs
        paths = load_configs()
        bucket_name = setup_gcp_bd()
        local_path = Path(paths["paths"]["gold"])
        layer = paths["layers"]["gold"]
        local_path.mkdir(parents=True,
                         exist_ok=True)

        # Statistical tests → JSON
        save_data(diagnostics_tests,
                  "diagnostics_tests",
                  directory=local_path,
                  file_format="json")
        save_data_to_gcs(diagnostics_tests,
                         "diagnostics_tests",
                         bucket_name,
                         layer=layer,
                         file_format="json")
        # Describe + correlation → Parquet
        save_data(diagnostics_summary_df,
                  "diagnostics_summary",
                  directory=local_path,
                  file_format="parquet")
        save_data_to_gcs(diagnostics_summary_df,
                         "diagnostics_summary",
                         bucket_name,
                         layer=layer,
                         file_format="parquet")
        return diagnostics_tests, diagnostics_summary_df
    except Exception as e:
        logger.error(f"Error diagnosing data: {e}")
        raise