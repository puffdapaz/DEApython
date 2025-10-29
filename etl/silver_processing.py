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
from .geodata import fetch_geodata, merge_geodata
from .diagnostics.data_validation import silver_schema, validate_data
from .diagnostics.model_diagnostics import analyze_data

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
        with open("configs/queries.yml",
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
    Add municipality-level completeness flag across years.
    Args:
        silver_df (pd.DataFrame): Silver layer DataFrame.
        value_columns (List[str]): Columns to check for completeness.
    Returns:
        pd.DataFrame: Silver DataFrame with 'is_complete_grouped' flag.
    """
    try:
        silver_df = silver_df.copy()
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
def validate_silver(silver_df: pd.DataFrame) -> None:
    """
    Validate silver layer DataFrame against predefined schema.
    Args:
        dataframe: DataFrame keyed by dataset name.
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
        dataframes: Dictionary of DataFrames to save
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

# ---------------------------------------------------------------------
# Main Workflow Function
# ---------------------------------------------------------------------
def process_silver_data() -> Optional[bd.Table]:
    """
    Orchestrate silver layer data processing workflow.
    Returns:      
        dataFrame: Resulting DataFrame, None otherwise
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

        geo_df = fetch_geodata(year=2017)
        silver_df = merge_geodata(silver_df, geo_df)

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
        silver_df = None

if __name__ == "__main__":
    silver_df = process_silver_data()