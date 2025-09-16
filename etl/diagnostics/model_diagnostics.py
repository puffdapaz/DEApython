import numpy as np
import yaml
from scipy import stats
import logging
from typing import Dict, Tuple, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_configs() -> tuple:
    """Load configurations from YAML files."""
    try:
        with open("configs/dea_config.yml", "r") as f:
            dea_config = yaml.safe_load(f)
        with open("configs/path.yml", "r") as f:
            paths = yaml.safe_load(f)
        return dea_config, paths
    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML config: {e}")
        raise

def shapiro_wilk_test(efficiency_scores: np.ndarray, alpha: float = 0.05) -> Dict[str, Any]:
    """
    Perform Shapiro-Wilk test for normality on efficiency scores.
    
    Parameters:
    -----------
    efficiency_scores : np.ndarray
        Array of efficiency scores
    alpha : float
        Significance level
        
    Returns:
    --------
    Dict with test results
    """
    try:
        stat, p_value = stats.shapiro(efficiency_scores)
        is_normal = p_value > alpha
        
        return {
            'test': 'Shapiro-Wilk',
            'statistic': stat,
            'p_value': p_value,
            'is_normal': is_normal,
            'alpha': alpha,
            'interpretation': 'Data is normally distributed' if is_normal else 'Data is not normally distributed'
        }
    except Exception as e:
        logger.error(f"Shapiro-Wilk test failed: {e}")
        return {'error': str(e)}

def kolmogorov_smirnov_test(sample1: np.ndarray, sample2: np.ndarray, 
                           alpha: float = 0.05) -> Dict[str, Any]:
    """
    Perform Kolmogorov-Smirnov test to compare two distributions.
    
    Useful for comparing CRS vs VRS efficiency scores.
    """
    try:
        stat, p_value = stats.ks_2samp(sample1, sample2)
        different_distributions = p_value < alpha
        
        return {
            'test': 'Kolmogorov-Smirnov',
            'statistic': stat,
            'p_value': p_value,
            'different_distributions': different_distributions,
            'alpha': alpha,
            'interpretation': 'Samples from different distributions' if different_distributions 
                            else 'Samples from same distribution'
        }
    except Exception as e:
        logger.error(f"KS test failed: {e}")
        return {'error': str(e)}

def efficiency_normality_test(efficiency_scores: Dict[str, np.ndarray], 
                             alpha: float = 0.05) -> Dict[str, Any]:
    """
    Test normality of all efficiency score distributions.
    """
    results = {}
    for model_name, scores in efficiency_scores.items():
        results[model_name] = shapiro_wilk_test(scores, alpha)
    
    return results

def returns_to_scale_test(crs_scores: np.ndarray, vrs_scores: np.ndarray,
                         drs_scores: np.ndarray, irs_scores: np.ndarray,
                         alpha: float = 0.05) -> Dict[str, Any]:
    """
    Test for significant differences between returns to scale models.
    """
    tests = {}
    
    # Test CRS vs VRS (scale efficiency)
    tests['crs_vs_vrs'] = kolmogorov_smirnov_test(crs_scores, vrs_scores, alpha)
    
    # Test IRS vs DRS for increasing vs decreasing returns
    tests['irs_vs_drs'] = kolmogorov_smirnov_test(irs_scores, drs_scores, alpha)
    
    return tests

def scale_efficiency_test(scale_efficiencies: np.ndarray, alpha: float = 0.05) -> Dict[str, Any]:
    """
    Test if scale efficiencies are significantly different from 1.
    """
    try:
        # One-sample t-test against mean of 1 (perfect scale efficiency)
        t_stat, p_value = stats.ttest_1samp(scale_efficiencies, 1.0)
        significantly_different = p_value < alpha
        
        return {
            'test': 'One-sample t-test (mean=1)',
            't_statistic': t_stat,
            'p_value': p_value,
            'significantly_different': significantly_different,
            'alpha': alpha,
            'mean_scale_efficiency': np.mean(scale_efficiencies),
            'interpretation': 'Scale efficiency significantly different from 1' if significantly_different
                            else 'No significant difference from perfect scale efficiency'
        }
    except Exception as e:
        logger.error(f"Scale efficiency test failed: {e}")
        return {'error': str(e)}