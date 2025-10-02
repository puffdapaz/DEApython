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

def load_configs() -> dict:
    """Load configuration from path.yml"""
    try:
        with open("configs/path.yml", "r") as f:
            paths = yaml.safe_load(f)
        return paths
    except Exception as e:
        logger.error(f"Error loading configs: {e}")
        raise

def setup_gcp_bd() -> str:
    """GCP credentials configuration to access Base dos Dados."""
    load_dotenv()
    billing_project_id = os.getenv("billing_project_id")
    bucket_name = os.getenv("gcp_bucket_name")
    
    if not billing_project_id or not bucket_name:
        raise ValueError("Missing required environment variables")
    return bucket_name

# ------------------
# Descriptive Analysis
# ------------------

def analyze_data(df: pd.DataFrame, name: str) -> None:
    if df is None:
        logger.warning(f"No DataFrame provided for {name} analysis.")
        return
    print(f"{name} data Analysis:")
    for year in sorted(df['ano'].unique()):
        year_data = df[df['ano'] == year]
        print(year, f"{name} records:", len(year_data))
        print("\n%s", year_data.describe().to_string())
        print("\n%s", year_data.corr(numeric_only=True).to_string())

# ------------------
# Statistical Tests
# ------------------

def shapiro_wilk_function(efficiency_scores: np.ndarray, alpha: float = 0.05) -> Dict[str, Any]:
    """Shapiro-Wilk test for normality"""
    try:
        stat, p_value = stats.shapiro(efficiency_scores)
        reject_null = p_value > alpha
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

def kolmogorov_smirnov_function(sample1: np.ndarray, sample2: np.ndarray, alpha: float = 0.05) -> Dict[str, Any]:
    """Two-sample KS test"""
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

def scale_efficiency_function(scale_efficiencies: np.ndarray, alpha: float = 0.05) -> Dict[str, Any]:
    """One-sample t-test against mean=1 (unchanged format)."""
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

def run_diagnostics(gold_df: pd.DataFrame, *, log: bool = True, alpha: float = 0.05) -> tuple[dict, pd.DataFrame]:
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
            year_gold_df["DEA_scale_efficiency"].to_numpy(),
            alpha=alpha
        )
        year_tests["ks_crs_vs_vrs"] = kolmogorov_smirnov_function(
            year_gold_df["DEA_crs_input"].to_numpy(),
            year_gold_df["DEA_vrs_input"].to_numpy(),
            alpha=alpha
        )
        year_tests["scale_eff"] = scale_efficiency_function(
            year_gold_df["DEA_scale_efficiency"].to_numpy(),
            alpha=alpha
        )
        year_tests["returns_to_scale"] = {
            "crs_vs_vrs": kolmogorov_smirnov_function(
                year_gold_df["DEA_crs_input"].to_numpy(),
                year_gold_df["DEA_vrs_input"].to_numpy(),
                alpha=alpha
            ),
            "irs_vs_drs": kolmogorov_smirnov_function(
                year_gold_df["DEA_irs_input"].to_numpy(),
                year_gold_df["DEA_drs_input"].to_numpy(),
                alpha=alpha
            ),
        }

        diagnostics_tests[year] = year_tests

        # Descriptive + correlation
        desc = year_gold_df.describe().reset_index()
        desc['ano'] = year 
        diagnostics_summary.append(desc)

        corr = year_gold_df.corr(numeric_only=True).reset_index()
        corr['ano'] = year
        diagnostics_summary.append(corr)

        if log:
            log_test_results(f"Year {year}", year_tests)

    diagnostics_summary_df = pd.concat(diagnostics_summary, ignore_index=True)

    # Save files locally & GCS
    paths = load_configs()
    bucket_name = setup_gcp_bd()
    local_path = Path(paths["paths"]["gold"])
    layer = paths["layers"]["gold"]
    local_path.mkdir(parents=True,
                     exist_ok=True)

    # 1. Statistical tests → JSON
    save_data(diagnostics_tests,
              "diagnostics_tests",
              directory=local_path,
              file_format="json")
    save_data_to_gcs(diagnostics_tests,
                     "diagnostics_tests",
                     bucket_name,
                     layer=layer,
                     file_format="json")

    # 2. Describe + correlation → Parquet
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