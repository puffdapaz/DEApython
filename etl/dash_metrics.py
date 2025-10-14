"""
Benchmarking and analytical features for DEA results.
This module creates derived features for comparative analysis.
"""
import pandas as pd
import logging

# ---------------------------------------------------------------------
# Config Loading
# ---------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Derived Fields Functions
# ---------------------------------------------------------------------
def eff_rank(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate yearly rankings based on VRS Input and Scale Efficiency.
    Args:
        gold_df: Gold DataFrame with DEA results
    Returns:
        DataFrame with ranking columns added
    """
    try:
        df = df.copy()
        df = df.sort_values(
            by=["year", 
                "DEA_vrs_input", 
                "DEA_scale_efficiency"],
            ascending=[True, 
                       False, 
                       False],
            ignore_index=True)
        # Create group-based rank
        df["rank_vrs"] = (
            df.groupby("year")
              .cumcount() + 1) # 1-based rank
        return df
    except Exception as e:
        logger.error(f"Error in eff_rank: {e}")
        raise

def yoy_variance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate variance between 2017 and 2019 for key metrics.
    Args:
        gold_df: Gold DataFrame with DEA results
    Returns:
        DataFrame with variance columns added
    """
    try:
        df = df.copy()
        df["pct_var_vrs"] = df.groupby("city_id")["DEA_vrs_input"].pct_change()
        df["pct_var_scale"] = df.groupby("city_id")["DEA_scale_efficiency"].pct_change()
        return df
    except Exception as e:
        logger.error(f"Error in yoy_variance: {e}")
        raise

def efficiency_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Categorize municipalities into efficiency bins.
    Args:
        gold_df: Gold DataFrame with DEA results
    Returns:
        DataFrame with efficiency bin columns added
    """
    try:
        df = df.copy()
        bins = [0,
                0.45,
                0.85,
                1.0]
        labels = ["Inefficient",
                  "Moderate",
                  "Efficient"]

        df["category_vrs"] = pd.cut(df["DEA_vrs_input"],
                                    bins=bins,
                                    labels=labels,
                                    include_lowest=True)
        df["category_scale"] = pd.cut(df["DEA_scale_efficiency"],
                                      bins=bins,
                                      labels=labels,
                                      include_lowest=True)
        return df
    except Exception as e:
        logger.error(f"Error in efficiency_category: {e}")
        raise

def median_deltas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate deviations from national median for key metrics.
    Args:
        gold_df: Gold DataFrame with DEA results
    Returns:
        DataFrame with deviation columns added
    """
    try:
        df = df.copy()
        df["delta_vrs_median"] = df["DEA_vrs_input"] - df.groupby("year")["DEA_vrs_input"].transform("median")
        df["delta_scale_median"] = df["DEA_scale_efficiency"] - df.groupby("year")["DEA_scale_efficiency"].transform("median")
        return df
    except Exception as e:
        logger.error(f"Error in median_deltas: {e}")
        raise

def state_benchmarks(df: pd.DataFrame,
                     gold_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate state-level benchmarks and comparisons.
    Args:
        gold_df: Gold DataFrame with DEA results
    Returns:
        DataFrame with regional benchmark columns added
    """
    try:
        df = df.copy()
        if "state" in gold_df.columns:
            state_avg = (gold_df.groupby(["year",
                                          "state"])
                               [["DEA_vrs_input",
                                 "DEA_scale_efficiency"]]
                                .mean()
                                .reset_index()
                                .rename(columns={"DEA_vrs_input": "state_avg_vrs",
                                                 "DEA_scale_efficiency": "state_avg_scale"}))
            df = df.merge(state_avg, on=["year",
                                         "state"],
                                     how="left")
        return df
    except Exception as e:
        logger.error(f"Error in regional_benchmarks: {e}")
        raise

def peer_groups(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create peer groups based on municipality characteristics.
    Args:
        gold_df: Gold DataFrame with municipality data
    Returns:
        DataFrame with peer group assignments
    """
    try:
        df = df.copy()
        if "population" in df.columns:
            df["peer_group"] = pd.qcut(df["population"], 4, labels=["Small", "Medium", "Large", "Mega"])
        return df
    except Exception as e:
        logger.error(f"Error in peer_groups: {e}")
        raise

# ---------------------------------------------------------------------
# Main Workflow Function
# ---------------------------------------------------------------------
def analytical_features(df: pd.DataFrame,
                        enriched: pd.DataFrame) -> pd.DataFrame:
    """
    Main function to add all analytical features to gold DataFrame.
    Args:
        gold_df: Gold DataFrame with DEA results
    Returns:
        Enhanced DataFrame with all analytical features
    """
    print("Calculating metrics...")
    try:
        enriched = df.copy()
        enriched = eff_rank(enriched)
        enriched = yoy_variance(enriched)
        enriched = efficiency_category(enriched)
        enriched = median_deltas(enriched)
        enriched = state_benchmarks(enriched, enriched)
        enriched = peer_groups(enriched)
        return enriched
    except Exception as e:
        logger.error(f"Error in analytical_features: {e}")
        raise