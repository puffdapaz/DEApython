"""
Warehouse Loader for DEApython Analytics Pipeline.
Uploads Gold layer DEApython outputs to a Neon PostgreSQL database
for BI visualization.

Architecture:
- SQLAlchemy: DDL / schema management
- psycopg: bulk loading
"""

import os
import logging
from pathlib import Path
import io
import pandas as pd
import yaml
import psycopg
from sqlalchemy import create_engine, text
from sqlalchemy import MetaData, Table, Column
from sqlalchemy.sql.sqltypes import BigInteger, Float, String

# Set Up Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
def load_configs(config_path: str = "configs/path.yaml") -> dict:
    """Load YAML configuration for paths and layers."""
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

# SQLAlchemy DSN (DDL, metadata)
SQLALCHEMY_DSN = (f"postgresql://{NEON_USER}:{NEON_PASSWORD}"
                  f"@{NEON_HOST}:{NEON_PORT}/{NEON_DATABASE}"
)

# psycopg DSN (COPY)
PSYCOPG_DSN = (f"host={NEON_HOST} "
               f"port={NEON_PORT} "
               f"dbname={NEON_DATABASE} "
               f"user={NEON_USER} "
               f"password={NEON_PASSWORD} "
               f"sslmode=require"
)

# ---------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------
def create_engine_sa():
    """Create SQLAlchemy engine (DDL / metadata only)."""
    return create_engine(SQLALCHEMY_DSN,
                         connect_args={"sslmode": "require"}
                        )
        
def drop_objects(engine):
    """
    Drops ALL user tables and views in the connected Postgres database.
    Use with caution (intended for dev / rebuild scenarios).
    """
    try:
        with engine.begin() as conn:
            # Drop views
            views = conn.execute(text("""
                SELECT table_schema, table_name
                FROM information_schema.views
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema');
            """)).fetchall()

            for schema, view in views:
                conn.execute(
                    text(f'DROP VIEW IF EXISTS "{schema}"."{view}" CASCADE;')
                )

            # Drop tables
            tables = conn.execute(text("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                  AND table_schema NOT IN ('pg_catalog', 'information_schema');
            """)).fetchall()

            for schema, table in tables:
                conn.execute(
                    text(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE;')
                )

    except Exception as e:
        logger.error(f"Failed to drop existing objects: {e}")
        raise

def infer_sqlalchemy_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return BigInteger
    if pd.api.types.is_float_dtype(dtype):
        return Float
    return String

def create_table_from_dataframe(engine, 
                                df: pd.DataFrame, table_name: str):
    metadata = MetaData()

    columns = [Column(col, infer_sqlalchemy_type(dtype))
               for col, dtype in df.dtypes.items()
              ]

    table = Table(table_name, 
                  metadata, 
                  *columns)
    metadata.drop_all(engine, 
                      tables=[table])
    metadata.create_all(engine)

# ---------------------------------------------------------------------
# COPY loader
# ---------------------------------------------------------------------
def copy_df_to_postgres(df: pd.DataFrame,
                        table_name: str,
                        dsn: str
                       ) -> None:
    """
    High-performance bulk load using Postgres COPY FROM STDIN (psycopg3).
    """
    buffer = io.StringIO()
    df.to_csv(buffer, 
              index=False, 
              header=False)
    buffer.seek(0)

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            with cur.copy(
                f"COPY {table_name} FROM STDIN WITH (FORMAT CSV)"
            ) as copy:
                copy.write(buffer.getvalue())

# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------
def load_to_neon(df: pd.DataFrame = None,
                 table_name: str = "dea_python") -> bool:
    try:
        if df is None:
            df = pd.read_parquet(DATA_PATH)

        engine = create_engine_sa()
        drop_objects(engine)
        create_table_from_dataframe(engine, df, table_name)
        copy_df_to_postgres(df, table_name, PSYCOPG_DSN)

        return True
    except Exception as e:
        logger.error(f"Failed to load data to data warehouse: {e}")
        raise