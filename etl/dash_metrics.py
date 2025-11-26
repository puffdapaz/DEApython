"""
Benchmarking and analytical feature engineering for DEA results.
Used to enrich the Gold layer dataset with comparative, temporal, and categorical indicators.
"""
import logging
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------
# Config Loading
# ---------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Frontier Plot Functions
# ---------------------------------------------------------------------
def frontier_composites(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build DEA composite input/output metrics for frontier plotting.
    Works even if the number of outputs varies by year.
    """
    try:
        df = df.copy()

        # 1. Extract input weights (always 2)
        vy = df[[c for c in df.columns if c.startswith("DEA_vrs_input_vy_")]].to_numpy()
        # 2. Extract output weights (variable length)
        ux = df[[c for c in df.columns if c.startswith("DEA_vrs_input_ux_")]].to_numpy()
        # 3. Extract output columns in the SAME order you created Y
        outputs = np.column_stack([df["ideb_initial_years"],
                                   df["ideb_final_years"],
                                   100 - df["dropout_rates_initial_years"],
                                   100 - df["dropout_rates_final_years"]
                                ])
        # But ux may have fewer columns → select only the first ux.shape[1]
        outputs = outputs[:, :ux.shape[1]]

        # 4. Compute weighted composites
        weighted_inputs = (df["gdp_per_capita"] * vy[:, 0] +
                           df["spending_per_student"] * vy[:, 1]
                        )
        weighted_outputs = (outputs * ux).sum(axis=1)

        df["DEA_weighted_input"] = weighted_inputs
        df["DEA_weighted_output"] = weighted_outputs
        df["DEA_frontier_output"] = (df["DEA_scale_efficiency"] * df["DEA_weighted_output"]
                                    )
        return df
    except Exception as e:
        logger.error(f"Error in data ranking: {e}")
        raise
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
        enriched = frontier_composites(enriched)
        enriched = yoy_variance(enriched)
        enriched = efficiency_category(enriched)
        enriched = median_deltas(enriched)
        enriched = eff_rank(enriched)
        enriched = state_benchmarks(enriched, enriched)
        enriched = (enriched.sort_values(["city_id", "year"])
                            .reset_index(drop=True))
        return enriched
    except Exception as e:
        logger.error(f"Error in analytical_features: {e}")
        raise