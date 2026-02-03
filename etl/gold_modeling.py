"""
Gold Layer Data Modeling Workflow
Handles DEA modeling, validation, and diagnostics.
This layer transforms curated Silver data into analytical Gold outputs
for business intelligence consumption and visualization.
"""
import os
import logging
from pathlib import Path
from typing import Optional, Tuple, Dict, List
import yaml
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from dealib import RTS, Orientation, dea
from sklearn.preprocessing import MinMaxScaler
from .dash_metrics import analytical_features
from .diagnostics.data_validation import gold_schema, validate_data
from .diagnostics import model_diagnostics as md
from .save_utils import save as save

# Set Up Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
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
    Configure GCP environment for Base dos Dados access.
    Sets up billing project ID and retrieves GCS bucket name from environment variables.
    Returns:
        str: GCS bucket name for data storage
    Raises:
        ValueError: If required environment variables are missing
    """
    load_dotenv()
    billing_project_id = os.getenv("billing_project_id")
    bucket_name = os.getenv("gcp_bucket_name")
    if not billing_project_id or not bucket_name:
        raise ValueError("Missing required environment variables")
    return bucket_name

def load_gold_data() -> pd.DataFrame:
    """
    Load processed silver data for DEA analysis
    Returns:
        dataFrame: Silver layer data with completeness flags
    Raises:
        Exception: For other unexpected errors
    """
    try:
        df_full = pd.read_parquet("data/processed/silver/silver_data.parquet")
        return df_full
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

# ---------------------------------------------------------------------
# Data Transformation Functions
# ---------------------------------------------------------------------
def use_completeness_flags(df_full: pd.DataFrame) -> pd.DataFrame:
    """
    Filter dataset to use only complete cases for DEA analysis.
    Args:
        df_full: Silver DataFrame with completeness flags
    Returns:
        dataFrame containing only complete cases
    Raises:
        ValueError: If no complete cases are found
    """
    try:
        df_complete = df_full[df_full["is_complete_grouped"] == True]
        if df_complete.empty:
            raise ValueError("No complete cases found in dataset")
        return df_complete
    except Exception as e:
        logger.error(f"Error filtering data: {e}")
        raise

def prepare_matrices(subset: pd.DataFrame)-> Tuple[np.ndarray, np.ndarray]:
    """
    Prepare and scale input and output matrices for DEA analysis.
    Args:
        df: DataFrame containing the data for DEA
    Returns:
        Tuple of (X, Y) where X is input matrix and Y is output matrix
    Raises:
        Exception: For other unexpected errors
    """
    try:
        X = subset[["gdp_per_capita",
                    "spending_per_student"]].to_numpy()
        y_ideb = subset[["ideb_initial_years",
                         "ideb_final_years"]].to_numpy()
        abandono_iniciais = (100 - subset["dropout_rates_initial_years"]).to_numpy().reshape(-1, 1)
        abandono_finais = (100 - subset["dropout_rates_final_years"]).to_numpy().reshape(-1, 1)
        Y = np.hstack([y_ideb,
                       abandono_iniciais,
                       abandono_finais])
        
        scaler_X = MinMaxScaler()
        scaler_Y = MinMaxScaler()

        X = scaler_X.fit_transform(X)
        Y = scaler_Y.fit_transform(Y)

        return X, Y
    except Exception as e:
            logger.error(f"Error preparing DEA matrices: {e}")
            raise

# ---------------------------------------------------------------------
# DEA Modeling Functions
# ---------------------------------------------------------------------
def run_dea_models(X: np.ndarray,
                   Y: np.ndarray) -> Dict[str, np.ndarray]:
    """
    Run DEA models with multiple returns-to-scale (RTS) assumptions.
    Args:
        X (np.ndarray): Input matrix.
        Y (np.ndarray): Output matrix.
    Returns:
        Dict[str, np.ndarray]: Efficiency scores by model specification.
    Raises:
        Exception: For other unexpected errors
    """
    dea_results = {}
    try:
        # CRS
        crs_input  = dea(X, Y, rts=RTS.crs, orientation=Orientation.input)
        # crs_output = dea(X, Y, rts=RTS.crs, orientation=Orientation.output)
        # VRS
        vrs_input  = dea(X, Y, rts=RTS.vrs, orientation=Orientation.input)
        # vrs_output = dea(X, Y, rts=RTS.vrs, orientation=Orientation.output)
        # IRS / DRS
        irs_input  = dea(X, Y, rts=RTS.irs, orientation=Orientation.input)
        drs_input  = dea(X, Y, rts=RTS.drs, orientation=Orientation.input)

        # Assign efficiency
        dea_results["crs_input"] = crs_input.eff
        # dea_results["crs_output"] = 1.0 / crs_output.eff
        dea_results["vrs_input"] = vrs_input.eff
        # dea_results["vrs_output"] = 1.0 / vrs_output.eff
        dea_results["irs_input"] = irs_input.eff
        dea_results["drs_input"] = drs_input.eff

        return dea_results
    except Exception as e:
        logger.error(f"Error running DEA models: {e}")
        raise

def derived_metrics(subset: pd.DataFrame,
                    dea_results: Dict[str, np.ndarray]) -> pd.DataFrame:
    """
    Add derived DEA metrics and scale efficiency to the dataset.
    Args:
        subset (pd.DataFrame): Input DataFrame.
        dea_results (Dict[str, np.ndarray]): Dictionary of efficiency scores.
    Returns:
        dataframe: DataFrame with added DEA_* columns.
    Raises:
        Exception: For other unexpected errors
    """
    try:
        result_df = subset.copy()
        eff = dict(dea_results)
        eff["scale_efficiency"] = eff["crs_input"] / eff["vrs_input"]

        returns_nature = np.where(
            eff["crs_input"] == eff["vrs_input"], "Constant",
            np.where(eff["drs_input"] == eff["vrs_input"], "Decreasing", "Increasing"))
        for k, arr in eff.items():
            result_df[f"DEA_{k}"] = arr
        result_df["DEA_returns_nature"] = returns_nature
        return result_df
    except Exception as e:
        logger.error(f"Error calculating derived metrics: {e}")
        raise

def results_wrapper(df_full: pd.DataFrame,
                    dea_results: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Merge DEA results back into the full DataFrame.
    Args:
        df_full dataFrame: Full silver dataset.
        dea_results dataFrame: DataFrame with DEA metrics.
    Returns:
        dataFrame: Enriched gold DataFrame.
    Raises:
        Exception: For other unexpected errors
    """
    try:
        merge_cols = [c for c in dea_results.columns if c not in df_full.columns] + ["city_id", "year"]
        gold_df = df_full.merge(
            dea_results[merge_cols],
            on=["city_id",
                "year"],
            how="left")
        gold_df["city_id"] = gold_df["city_id"].astype(str)
        return gold_df
    except Exception as e:
        logger.error(f"Error merging DEA results: {e}")
        raise

def run_gold_model(df_full: pd.DataFrame) -> List[pd.DataFrame]:
    """
    Run DEA analysis by year and return merged gold dataset.
    Args:
        df_full dataFrame: Full silver DataFrame.
    Returns:
        dataFrame: Gold DataFrame with DEA results
    Raises:
        Exception: For other unexpected errors
    """
    try:
        df_complete = use_completeness_flags(df_full)
        results = []
        
        for year, year_data in df_complete.groupby("year"):
            print(f"Running DEA for {year}")  
            try:
                X, Y = prepare_matrices(year_data)
                dea_results = run_dea_models(X, Y)
                year_results = derived_metrics(year_data, dea_results)
                results.append(year_results)
            except Exception as e:
                    logger.error("DEA failed for year %d: %s", year, e)
                    raise
        results_df = pd.concat(results, ignore_index=True)
        gold_df = results_wrapper(df_full, results_df)
        return gold_df
    except Exception as e:
        logger.error(f"Error running model: {e}")
        raise

# ---------------------------------------------------------------------
# Validation, Diagnostics & Saving
# ---------------------------------------------------------------------
def validate_gold(gold_df: pd.DataFrame) -> bool:
    """
    Validate gold layer DataFrame against predefined schema.
    Args:
        dataframe dataFrame: DataFrame keyed by dataset name.
    Returns:
        bool: True if validation passes, False otherwise.
    Raises:
        ValueError: If validation fails for any DataFrame.
    """
    if not validate_data(gold_df,
                         schema=gold_schema,
                         name="Gold"):
        raise ValueError("Gold data validation failed")  

def run_diagnostics(gold_df: pd.DataFrame) -> None:
    """
    Run diagnostics and profiling on gold data.
    Args:
        gold_df dataFrame: Gold DataFrame.
    Raises:
        Exception: For other unexpected errors
    """
    try:
        md.analyze_data(gold_df,
                        name="Gold")
        md.run_diagnostics(gold_df,
                           log=True)
    except Exception as e:
        logger.error(f"Error during gold data diagnostics: {e}")
        raise

def save_gold(gold_df: pd.DataFrame,
              paths: dict,
              bucket_name: str) -> None:
    """
    Save gold DataFrame and diagnostics locally and to GCS.
    Args:
        gold_df dataFrame: Silver DataFrame.
        paths dict: Dictionary with local paths and layers.
        bucket_name str: GCP bucket name.
    Raises:
        Exception: For other unexpected errors.
    """
    try:
        local_path = Path(paths["paths"]["gold"])
        layer = paths["layers"]["gold"]
        local_path.mkdir(parents=True,
                         exist_ok=True)
        save.save_data(gold_df,
                       "gold_data",
                       directory=local_path)
        save.save_data_to_gcs(gold_df,
                              "gold_data",
                              bucket_name,
                              layer=layer)
        logger.info(f"Data saved at {local_path} and GCP://{bucket_name}/{layer} successfully")
    except Exception as e:
        logger.error(f"Error saving {layer}: {e}")

# ---------------------------------------------------------------------
# Main Workflow Function
# ---------------------------------------------------------------------
def model_gold_data() -> Optional[pd.DataFrame]:
    """
    Orchestrate gold layer data modeling workflow.
    Returns:      
        dataFrame: Resulting DataFrame, None otherwise
    Raises:
        Exception: If any critical step in the workflow fails
    """
    try:
        paths = load_configs()
        bucket_name = setup_gcp_bd()

        df_full = load_gold_data()
        gold_df = run_gold_model(df_full)
        gold_df = analytical_features(gold_df)

        validate_gold(gold_df)
        run_diagnostics(gold_df)

        save_gold(gold_df, 
                  paths, 
                  bucket_name)
        print(f"Modeling completed")
        return gold_df
    except Exception as e:
            logger.error(f"Modeling failed: {e}")
            raise

if __name__ == "__main__":
    gold_df = model_gold_data()