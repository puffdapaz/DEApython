import numpy as np
from scipy import stats
import logging
from typing import Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ------------------
# Config & Analysis
# ------------------

def analyze_silver_data(df):
    """Generate analysis of silver data."""
    if df is None:
        logger.warning("No DataFrame provided for analysis.")
        return
    logger.info("🔎 Silver Data Analysis:")
    logger.info(f"Total records: {len(df)}")


def analyze_gold_data(gold_df):
    """Generate analysis of gold data."""
    if gold_df is None:
        logger.warning("No DataFrame provided for analysis.")
        return
    logger.info("🔎 Gold Data Analysis:")
    logger.info(f"Total records: {len(gold_df)}")


# ------------------
# Statistical Tests
# ------------------

def shapiro_wilk_test(efficiency_scores: np.ndarray, alpha: float = 0.05) -> Dict[str, Any]:
    """Shapiro-Wilk test for normality."""
    if efficiency_scores is not None:
        try:
            stat, p_value = stats.shapiro(efficiency_scores)
            is_normal = p_value > alpha
            return {
                "test": "Shapiro-Wilk",
                "statistic": stat,
                "p_value": p_value,
                "is_normal": is_normal,
                "alpha": alpha,
                "interpretation": (
                    "Data is normally distributed" if is_normal else "Data is not normally distributed"
                ),
            }
        except Exception as e:
            logger.error(f"Shapiro-Wilk test failed: {e}")
            return {"error": str(e)}


def kolmogorov_smirnov_test(
    sample1: np.ndarray, sample2: np.ndarray, alpha: float = 0.05
) -> Dict[str, Any]:
    """Kolmogorov-Smirnov test to compare two distributions."""
    try:
        stat, p_value = stats.ks_2samp(sample1, sample2)
        different_distributions = p_value < alpha
        return {
            "test": "Kolmogorov-Smirnov",
            "statistic": stat,
            "p_value": p_value,
            "different_distributions": different_distributions,
            "alpha": alpha,
            "interpretation": (
                "Samples from different distributions"
                if different_distributions
                else "Samples from same distribution"
            ),
        }
    except Exception as e:
        logger.error(f"KS test failed: {e}")
        return {"error": str(e)}


def efficiency_normality_test(
    efficiency_scores: Dict[str, np.ndarray], alpha: float = 0.05
) -> Dict[str, Any]:
    """Test normality of all efficiency score distributions."""
    results = {}
    for model_name, scores in efficiency_scores.items():
        results[model_name] = shapiro_wilk_test(scores, alpha)
    return results


def returns_to_scale_test(
    crs_scores: np.ndarray,
    vrs_scores: np.ndarray,
    drs_scores: np.ndarray,
    irs_scores: np.ndarray,
    alpha: float = 0.05,
) -> Dict[str, Any]:
    """Test for significant differences between returns to scale models."""
    return {
        "crs_vs_vrs": kolmogorov_smirnov_test(crs_scores, vrs_scores, alpha),
        "irs_vs_drs": kolmogorov_smirnov_test(irs_scores, drs_scores, alpha),
    }


def scale_efficiency_test(
    scale_efficiencies: np.ndarray, alpha: float = 0.05
) -> Dict[str, Any]:
    """One-sample t-test against mean=1 (perfect scale efficiency)."""
    try:
        t_stat, p_value = stats.ttest_1samp(scale_efficiencies, 1.0)
        significantly_different = p_value < alpha
        return {
            "test": "One-sample t-test (mean=1)",
            "t_statistic": t_stat,
            "p_value": p_value,
            "significantly_different": significantly_different,
            "alpha": alpha,
            "mean_scale_efficiency": np.mean(scale_efficiencies),
            "interpretation": (
                "Scale efficiency significantly different from 1"
                if significantly_different
                else "No significant difference from perfect scale efficiency"
            ),
        }
    except Exception as e:
        logger.error(f"Scale efficiency test failed: {e}")
        return {"error": str(e)}


# ------------------
# Logging
# ------------------

def log_test_results(test_name: str, results: Dict[str, Any], indent: int = 0):
    """Nicely format and log a single test result (handles nested dicts)."""
    prefix = " " * indent
    logger.info(f"{prefix}📊 {test_name} Results")
    for k, v in results.items():
        if isinstance(v, dict):
            # recursive logging for nested results
            log_test_results(k, v, indent + 3)
        else:
            if isinstance(v, float):
                logger.info(f"{prefix}   {k}: {v:.4f}")
            else:
                logger.info(f"{prefix}   {k}: {v}")
    if indent == 0:  # only separate top-level
        logger.info("-" * 40)
