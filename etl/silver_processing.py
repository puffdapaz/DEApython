"""
Silver Layer Data Processing Workflow
This module processes, validates, and saves transformed data
to both local storage and Google Cloud Storage (GCS).
"""
import os
import yaml
import logging
import pandas as pd
import basedosdados as bd
from typing import List, Optional
from pathlib import Path
from dotenv import load_dotenv
from .save_utils import save as save
from .geodata import geographical_features
from .diagnostics.data_validation import silver_schema, validate_data
from .diagnostics.model_diagnostics import analyze_data

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Config Loading
# ---------------------------------------------------------------------
def load_configs(config_path: str = "configs/path.yaml") -> dict:
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

def load_silver_data() -> str:
    """
    Load SQL query from YAML configuration file for silver layer.
    Returns:
        str: SQL query string for silver data extraction.
    Raises:
        Exception: For other unexpected errors
    """
    try:
        with open("configs/queries.yaml",
                  "r",
                  encoding="utf-8") as f:
            query = yaml.safe_load(f)
            return query["silver"]["query"]
    except Exception as e:
        logger.error(f"Error loading silver_query: {e}")
        raise

def run_silver_query(query: str) -> pd.DataFrame:
    """
    Execute silver SQL query using Base dos Dados.
    Args:
        query (str): SQL query string.
    Returns:
        pd.DataFrame: Resulting DataFrame.
    Raises:
        Exception: For other unexpected errors.
    """
    try:
        df = bd.read_sql(query,
                         billing_project_id=os.getenv("billing_project_id"))
        return df
    except Exception as e:
        logger.error(f"Error running query: {e}")
        raise

# ---------------------------------------------------------------------
# Data Transformation Functions
# ---------------------------------------------------------------------
def add_completeness_flags(silver_df: pd.DataFrame,
                           value_columns: List[str]) -> pd.DataFrame:
    """
    Args:
    silver_df: Silver layer DataFrame.
    value_columns: List of column names to check for completeness.
Returns:
    pd.DataFrame: DataFrame with an additional 'is_complete_grouped' flag per municipality.
    """
    try:
        silver_df = silver_df.copy()
        # Flag municipalities that have all non-null values across all value columns and years
        tmp_flag = silver_df[value_columns].notnull().all(axis=1)
        silver_df['is_complete_grouped'] = (silver_df.assign(_tmp=tmp_flag)
                                                    .groupby('city_id')['_tmp']
                                                    .transform(lambda x: x.all()))
        return silver_df
    except Exception as e:
        logger.error(f"Error categorizing data: {e}")
        raise

# ---------------------------------------------------------------------
# Validation & Saving
# ---------------------------------------------------------------------
def validate_silver(silver_df: pd.DataFrame) -> bool:
    """
    Validate silver layer DataFrame against predefined schema.
    Args:
        silver_df: Silver layer DataFrame to validate.
    Returns:
        bool: True if validation passes, False otherwise.
    Raises:
        ValueError: If validation fails for any DataFrame.
    """
    if not validate_data(silver_df,
                         schema=silver_schema,
                         name="Silver"):
            raise ValueError("silver data validation failed")       

def save_silver(silver_df: pd.DataFrame,
                paths: dict,
                bucket: str) -> None:
    """
    Save DataFrames to local storage and Google Cloud Storage.
    Args:
        silver_df: Silver layer DataFrame.
        paths: Local directory path for storage
        bucket: GCS bucket name
    Raises:
        Exception: For other unexpected errors.
    """
    try:
        local_path = Path(paths["paths"]["silver"])
        layer = paths["layers"]["silver"]
        local_path.mkdir(parents=True,
                         exist_ok=True)
        save.save_data(silver_df,
                       "silver_data",
                       directory=local_path)
        save.save_data_to_gcs(silver_df,
                              "silver_data",
                              bucket,
                              layer=layer)
        logger.info(f"Data saved at {local_path} and GCP://{bucket}/{layer} successfully")
    except Exception as e:
        logger.error(f"Error saving {layer}: {e}")
        raise

# ---------------------------------------------------------------------
# Main Workflow Function
# ---------------------------------------------------------------------
def process_silver_data() -> Optional[bd.Table]:
    """
    Orchestrate silver layer data processing workflow.
    Returns:      
        pd.DataFrame | None: The processed Silver dataset, or None if the pipeline fails.
    Raises:
        Exception: If any critical step in the workflow fails
    """
    try:
        paths = load_configs()
        bucket_name = setup_gcp_bd()

        query = load_silver_data()
        silver_df = run_silver_query(query)
        
        value_columns = ['population',
                         'gdp',
                         'education_spending',
                         'enrollments',
                         'ideb_initial_years',
                         'ideb_final_years',
                         'dropout_rates_initial_years',
                         'dropout_rates_final_years',
                         'gdp_per_capita',
                         'spending_per_student']
        silver_df = add_completeness_flags(silver_df,
                                           value_columns)

        geographical_features(year=2017)
        validate_silver(silver_df)
        analyze_data(silver_df,
                     name="silver")

        save_silver(silver_df,
                    paths,
                    bucket_name)
        print(f"Processing completed")
        return silver_df
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise

if __name__ == "__main__":
    silver_df = process_silver_data()