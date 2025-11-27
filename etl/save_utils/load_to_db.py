"""
Warehouse Loader for DEApython Analytics Pipeline.
Uploads Gold layer DEApython outputs to a Neon Postgres database
for BI visualization.
This module serves as the integration layer between the analytical 
pipeline and BI tools.
"""

import os
import logging
from pathlib import Path
import pandas as pd
import yaml
from sqlalchemy import create_engine

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Config
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

config = load_configs()
DATA_PATH = Path(config["paths"]["gold"]) / "gold_data.parquet"

NEON_USER = os.getenv("NEON_USER")
NEON_PASSWORD = os.getenv("NEON_PASSWORD")
NEON_HOST = os.getenv("NEON_HOST")
NEON_PORT = os.getenv("NEON_PORT", "5432")
NEON_DATABASE = os.getenv("NEON_DATABASE", "postgres")

# ---------------------------------------------------------------------
# Database Connection and Data Loading
# ---------------------------------------------------------------------
def create_neon_connection():
    """
    Create connection to Neon Postgres
    Args:
        cert_path : Path to save the SSL certificate locally
    Returns:
        sqlalchemy.engine.Engine: Active SQLAlchemy engine object.
    Raises:
        Exception: For other unexpected errors
    """
    connection_string = f"postgresql://{NEON_USER}:{NEON_PASSWORD}@{NEON_HOST}:{NEON_PORT}/{NEON_DATABASE}"
    
    engine = create_engine(connection_string,
                           connect_args={"sslmode": "require"})
    return engine

def load_to_neon(df: pd.DataFrame = None,
                 table_name: str = "DEA_python") -> bool:
    """
    Load DEApython Gold layer data to a Neon Postgres warehouse.
    Args:
        df (pd.DataFrame, optional): DataFrame to upload. If None, loads from DATA_PATH.
        table_name (str): Target table name in the Neon database.
    Returns:
        bool: True if the load succeeded, raises Exception otherwise.
    Raises:
        Exception: For other unexpected errors
    """
    try:
        df = pd.read_parquet(DATA_PATH)
        engine = create_neon_connection()
        
        with engine.begin() as conn:
            df.to_sql(table_name, 
                    conn, 
                    if_exists="replace", 
                    index=False, 
                    method="multi")
        print(f"Loaded {len(df)} rows to {table_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to load data to data warehouse: {e}")
        raise