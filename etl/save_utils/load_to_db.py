"""
Warehouse Loader
----------------
Uploads Gold layer DEApython outputs to a Neon Postgres database
for BI visualization (Power BI, Metabase, etc.).

Integration layer between the analytical pipeline and BI tools.
"""

import os
import logging
from pathlib import Path
import pandas as pd
import requests
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

# NEON_URL = os.getenv("NEON_URL", "postgres://user:password@host:port/dbname?sslmode=require")
NEON_USER = os.getenv("NEON_USER")
NEON_PASSWORD = os.getenv("NEON_PASSWORD")
NEON_HOST = os.getenv("NEON_HOST")
NEON_PORT = os.getenv("NEON_PORT", "5432")
NEON_DATABASE = os.getenv("NEON_DATABASE", "postgres")

# ---------------------------------------------------------------------
# SSL Certificate Handling
# ---------------------------------------------------------------------
# def neon_ssl_cert(cert_path: Path = CERT_PATH) -> Path:
#     """
#     Downloads SSL certificate for local SSL validation.
#     Args:
#         cert_path : Path to save the SSL certificate locally
#     Returns:
#         The path where the certificate is stored
#     Raises:
#         Exception: For other unexpected errors
#     """
#     cert_url = "https://neon.tech/api/v2/ssl/cert"
#     response = requests.get(cert_url, timeout=10)
#     response.raise_for_status()

#     with open(cert_path, "wb") as f:
#         f.write(response.content)
#     return cert_path

# ---------------------------------------------------------------------
# Database Connection and Data Loading
# ---------------------------------------------------------------------
def create_neon_connection():
    """
    Create connection to Neon Postgres
    Args:
        cert_path : Path to save the SSL certificate locally
    Returns:
        sqlalchemy.engine : SQLAlchemy engine object.
    Raises:
        Exception: For other unexpected errors
    """
    connection_string = f"postgresql://{NEON_USER}:{NEON_PASSWORD}@{NEON_HOST}:{NEON_PORT}/{NEON_DATABASE}"
    
    # For SSL handling
    engine = create_engine(connection_string,
                           connect_args={"sslmode": "require"})
    return engine

def load_to_neon(df: pd.DataFrame = None,
                 table_name: str = "DEA_python") -> bool:
    """
    Load your DEApython outputs to Neon
    Args:
        cert_path : Path to save the SSL certificate locally
    Returns:
        sqlalchemy.engine : SQLAlchemy engine object.
    Raises:
        Exception: For other unexpected errors
    """
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

if __name__ == "__main__":
    try:
        load_to_neon()
    except Exception as e:
        logger.exception("Failed to load data to data warehouse: %s", e)
        raise