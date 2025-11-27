"""
Benchmarking and analytical feature engineering for DEA results.
Used to enrich the Gold layer dataset with comparative, temporal, and categorical indicators.
"""
import logging
import pandas as pd

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
    Calculate yearly national and state rankings based on VRS Input and Scale Efficiency.
    Args:
        df: Gold DataFrame with DEA results
    Returns:
        pd.DataFrame: Input DataFrame with `rank_vrs` and `state_rank_vrs` column representing yearly rankings (descending efficiency order).
    Raises:
        Exception: For other unexpected errors.
    """
    try:
        # 1-based National ranking
        df = df.copy()
        df = df.sort_values(
            by=["year", 
                "DEA_vrs_input", 
                "DEA_scale_efficiency"],
            ascending=[True, 
                       False, 
                       False],
            ignore_index=True)
        df["rank_vrs"] = (df.groupby("year")
                            .cumcount() + 1)

        # 1-based State-level ranking
        df = df.sort_values(
            by=["year", 
                "state_name", 
                "DEA_vrs_input", 
                "DEA_scale_efficiency"],
            ascending=[True, 
                       True, 
                       False, 
                       False],
            ignore_index=True)
        df["state_rank_vrs"] = (df.groupby(["year",
                                            "state_name"])
                                  .cumcount() + 1)

        return df
    except Exception as e:
        logger.error(f"Error in data ranking: {e}")
        raise

def yoy_variance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate variance between 2017 and 2019 for key metrics.
    Args:
        df: Gold DataFrame with DEA results
    Returns:
        DataFrame with variance columns added
    Raises:
        Exception: For other unexpected errors.
    """
    try:
        df = df.copy()
        df["pct_var_vrs"] = df.groupby("city_id")["DEA_vrs_input"].pct_change()
        df["pct_var_scale"] = df.groupby("city_id")["DEA_scale_efficiency"].pct_change()
        return df
    except Exception as e:
        logger.error(f"Error in variance calculation: {e}")
        raise

def efficiency_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Categorize municipalities into efficiency bins.
    Args:
        df: Gold DataFrame with DEA results
    Returns:
        DataFrame with efficiency bin columns added
    Raises:
        Exception: For other unexpected errors.
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
        logger.error(f"Error in efficiency categorization: {e}")
        raise

def median_deltas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate deviations from national median for key metrics.
    Args:
        df: Gold DataFrame with DEA results
    Returns:
        DataFrame with deviation columns added
    Raises:
        Exception: For other unexpected errors.
    """
    try:
        df = df.copy()
        df["delta_vrs_median"] = df["DEA_vrs_input"] - df.groupby("year")["DEA_vrs_input"].transform("median")
        df["delta_scale_median"] = df["DEA_scale_efficiency"] - df.groupby("year")["DEA_scale_efficiency"].transform("median")
        return df
    except Exception as e:
        logger.error(f"Error in median calculation: {e}")
        raise

def state_benchmarks(df: pd.DataFrame,
                     gold_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate state-level benchmarks and comparisons.
    Args:
        df: Gold DataFrame with DEA results
    Returns:
        DataFrame with regional benchmark columns added
    Raises:
        Exception: For other unexpected errors.
    """
    try:
        df = df.copy()
        if "state_name" in gold_df.columns:
            state_avg = (gold_df.groupby(["year",
                                          "state_name"])
                            [["DEA_vrs_input",
                              "DEA_scale_efficiency"]]
                            .mean()
                            .reset_index()
                            .rename(columns={"DEA_vrs_input": "state_avg_vrs",
                                             "DEA_scale_efficiency": "state_avg_scale"}))
            df = df.merge(state_avg, on=["year",
                                         "state_name"],
                                     how="left")
        return df
    except Exception as e:
        logger.error(f"Error in benchmark calculation: {e}")
        raise

# ---------------------------------------------------------------------
# Main Workflow Function
# ---------------------------------------------------------------------
def analytical_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate all analytical feature functions into a single transformation pipeline.
    Args:
        df (pd.DataFrame): Gold layer DataFrame containing DEA results.
    Returns:
        pd.DataFrame: Enhanced DataFrame with analytical and benchmarking features.
    Raises:
        Exception: For other unexpected errors.
    """
    print("Calculating metrics...")
    try:
        enriched = df.copy()
        enriched = eff_rank(enriched)
        enriched = yoy_variance(enriched)
        enriched = efficiency_category(enriched)
        enriched = median_deltas(enriched)
        enriched = state_benchmarks(enriched, enriched)
        return enriched
    except Exception as e:
        logger.error(f"Error in analytical_features: {e}")
        raise