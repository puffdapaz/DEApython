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
        return enriched
    except Exception as e:
        logger.error(f"Error in analytical_features: {e}")
        raise