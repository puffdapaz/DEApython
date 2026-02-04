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
    Rows without DEA_vrs_input receive rank 0.
    Args:
        df: Gold DataFrame with DEA results
    Returns:
        pd.DataFrame: Input DataFrame with `rank_vrs` and `state_rank_vrs` column representing yearly rankings (descending efficiency order).
    Raises:
        Exception: For other unexpected errors.
    """
    try:
        df = df.copy()

        # Separate valid rows (have DEA score)
        valid = df[df["DEA_vrs_input"].notna()].copy()

        # National Rank
        valid = valid.sort_values(
            by=["year", "DEA_vrs_input", "DEA_scale_efficiency"],
            ascending=[True, False, False]
        )

        valid["rank_vrs"] = (
            valid.groupby("year")
                 .cumcount() + 1
        )

        # State Rank
        valid = valid.sort_values(
            by=["year", "state_name", "DEA_vrs_input", "DEA_scale_efficiency"],
            ascending=[True, True, False, False]
        )

        valid["state_rank_vrs"] = (
            valid.groupby(["year", "state_name"])
                 .cumcount() + 1
        )

        # Merge ranks back to original dataframe
        df = df.merge(
            valid[["city_id", "year", "rank_vrs", "state_rank_vrs"]],
            on=["city_id", "year"],
            how="left"
        )

        # Rows without DEA score → rank = 0
        df["rank_vrs"] = df["rank_vrs"].fillna(0).astype(int)
        df["state_rank_vrs"] = df["state_rank_vrs"].fillna(0).astype(int)

        return df
    except Exception as e:
        logger.error(f"Error in data ranking: {e}")
        raise

def national_median_vrs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add national median DEA metrics per year.
    These are reference values (NOT deltas) to be used in visuals
    such as bullet charts, reference lines, and KPI comparisons.
    Args:
        df: Gold DataFrame with DEA results
    Returns:
        pd.DataFrame: DataFrame with national median columns added
    Raises:
        Exception: For other unexpected errors.
    """
    try:
        df = df.copy()
        # National median VRS efficiency per year
        df["national_median_vrs"] = (df.groupby("year")["DEA_vrs_input"]
                                       .transform("median")
                                    )
        return df
    except Exception as e:
        logger.error(f"Error calculating national medians: {e}")
        raise

def state_avg_vrs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate state-level yearly average for DEA VRS Input efficiency.
    Args:
        df: Gold DataFrame containing DEA results with at least:
            - 'state' (state identifier)
            - 'year'
            - 'DEA_vrs_input'
    Returns:
        pd.DataFrame: Original DataFrame with new column:
            - 'state_avg_vrs'
    Raises:
        Exception: For other unexpected errors.
    """
    try:
        df = df.copy()
        df["state_avg_vrs"] = (df.groupby(["state_id", "year"])["DEA_vrs_input"]
                                 .transform("mean")
                            )
        return df
    except Exception as e:
        logger.error(f"Error calculating state average VRS: {e}")
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
        enriched = national_median_vrs(enriched)
        enriched = state_avg_vrs(enriched)
        return enriched
    except Exception as e:
        logger.error(f"Error in analytical_features: {e}")
        raise