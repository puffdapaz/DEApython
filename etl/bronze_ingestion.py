"""
Bronze Layer Data Ingestion Workflow.
This module handles the extraction of raw data from Base dos Dados and saves
to both local storage and Google Cloud Storage. It includes data validation
and comprehensive error handling for reliable data ingestion.
"""
import os
import yaml
import logging
import pandas as pd
import basedosdados as bd
from typing import Dict
from pathlib import Path
from dotenv import load_dotenv
from .save_utils import save as save
from .diagnostics.data_validation import schemas, validate_data

# Set up logging
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
        Exception: For other unexpected errors during file reading
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

def load_bronze_data(layer: str = "bronze") -> Dict[str, str]:
    """
    Load SQL queries from YAML configuration file for bronze layer.
    Args:
        layer: Bronze layer identifier
    Returns:
        Dictionary mapping query names to SQL query strings
    Raises:
        Exception: For other unexpected errors
    """
    try:
        with open("configs/queries.yml",
                  "r",
                  encoding="utf-8") as f:
            all_queries = yaml.safe_load(f)
        layer_queries = all_queries.get(f"{layer}_queries", {})
        if not layer_queries:
            logger.warning(f"No queries found for layer: {layer}")
            return {}
        return layer_queries
    except Exception as e:
        logger.error(f"Unexpected error loading queries: {e}")
        raise

# ---------------------------------------------------------------------
# Data Ingestion Functions
# ---------------------------------------------------------------------
def run_bronze_query(queries: Dict[str, str]) -> Dict[str, pd.DataFrame]:
    """
    Execute SQL queries against Base dos Dados and return results as DataFrames.
    Args:
        queries: Dictionary mapping query names to SQL query strings
    Returns:
        Dictionary mapping query names to their resulting DataFrames.
        Failed queries will have None as their value.
    Raises:
        Exception: For other unexpected errors.
    """
    dataframes = {}
    for name, query in queries.items():
        try:
            df = bd.read_sql(query,
                             billing_project_id=os.getenv("billing_project_id"))
            dataframes[name] = df
        except Exception as e:
            logger.error(f"Error running query {name}: {e}")
            dataframes[name] = None
    return dataframes

# ---------------------------------------------------------------------
# Validation & Saving
# --------------------------------------------------------------------
def validate_bronze(dataframes: Dict[str, pd.DataFrame]) -> None:
    """
    Validate bronze layer DataFrames against predefined schemas.
    Args:
        dataframes: Dictionary of DataFrames keyed by dataset name
    Raises:
        ValueError: If validation fails for any DataFrame.
    """
    if not validate_data(dataframes,
                         schema_map=schemas):
        raise ValueError("Bronze data validation failed")

def save_bronze(dataframes: Dict[str, pd.DataFrame],
                paths: dict,
                bucket: str) -> None:
    """
    Save DataFrames to local storage and Google Cloud Storage.
    Args:
        dataframes: Dictionary of DataFrames to save
        paths: Local directory path for storage
        bucket: GCS bucket name
    Raises:
        Exception: For other unexpected errors.
    """
    try:
        local_path = Path(paths["paths"]["bronze"])
        layer = paths["layers"]["bronze"]
        local_path.mkdir(parents=True,
                        exist_ok=True)

        for name, df in dataframes.items():
            if df is not None:
                save.save_data(df,
                               f"bronze_{name}",
                               directory=local_path)
                save.save_data_to_gcs(df,
                                      f"bronze_{name}",
                                      bucket,
                                      layer=layer)
                logger.info(f"Data saved at {local_path} and GCP://{bucket}/{layer} successfully")
    except Exception as e:
                logger.error(f"Error saving {layer}: {e}")

# ---------------------------------------------------------------------
# Main Workflow Function
# ---------------------------------------------------------------------
def ingest_bronze_data() -> Dict[str, pd.DataFrame]:
    """
    Orchestrate bronze layer data ingestion workflow.
    Returns:
        Dictionary of loaded DataFrames if successful, None otherwise
    Raises:
        Exception: If any critical step in the workflow fails
    """
    try:
        paths = load_configs()
        bucket_name = setup_gcp_bd()

        queries = load_bronze_data("bronze")
        dataframes = run_bronze_query(queries)

        validate_bronze(dataframes)

        save_bronze(dataframes,
                    paths,
                    bucket_name)
        print(f"Ingestion completed")
        return dataframes
    except Exception as e:
            logger.error(f"Ingestion failed: {e}")
            raise

if __name__ == "__main__":
    ingest_bronze_data()