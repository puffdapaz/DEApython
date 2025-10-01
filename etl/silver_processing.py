import basedosdados as bd
import os
import yaml
import logging
import pandas as pd
from .save_utils import save as save
from typing import List, Optional
from pathlib import Path
from dotenv import load_dotenv
from .diagnostics.data_validation import silver_schema, validate_data
from .diagnostics.model_diagnostics import analyze_data

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_configs() -> tuple:
    """Load configurations from YAML files."""
    try:
        with open("configs/dea_config.yml", "r") as f:
            dea_config = yaml.safe_load(f)
        with open("configs/path.yml", "r") as f:
            paths = yaml.safe_load(f)
        return dea_config, paths
    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML config: {e}")
        raise

def setup_basedosdados() -> str:
    """Set up Base dos Dados configuration and return bucket name."""
    load_dotenv()
    billing_project_id = os.getenv("billing_project_id")
    bucket_name = os.getenv("gcp_bucket_name")
    
    if not billing_project_id or not bucket_name:
        raise ValueError("Missing required environment variables")
    
    bd.config.billing_project_id = billing_project_id
    
    return bucket_name

def get_silver_query() -> str:
    """Return the silver layer SQL query from configs/queries.yml."""
    try:
        with open("configs/queries.yml", "r", encoding="utf-8") as f:
            query = yaml.safe_load(f)
            return query["silver"]["query"]
        if not query:
            raise ValueError("No silver_query found in queries.yml")
        return query
    except Exception as e:
        logger.error(f"Error loading silver_query: {e}")
        raise

def add_completeness_flags(silver_df: pd.DataFrame, value_columns: List[str]) -> pd.DataFrame:
    """Add municipality-level completeness flag across years."""

    silver_df = silver_df.copy()

    # Temporary row-level flag
    tmp_flag = silver_df[value_columns].notnull().all(axis=1)

    # Grouped completeness: all years must be complete
    silver_df['is_complete_grouped'] = (
        silver_df.assign(_tmp=tmp_flag)
          .groupby('id_municipio')['_tmp']
          .transform(lambda x: x.all())
    )

    return silver_df

def process_silver_data() -> Optional[bd.Table]:
    """Process silver layer data."""
    try:
        # Load configurations
        dea_config, paths = load_configs()
        
        # Set up Base dos Dados
        bucket_name = setup_basedosdados()
        
        # Get and execute query
        query = get_silver_query()
        silver_df = bd.read_sql(query)
        
        value_columns = [
        'populacao', 'pib', 'gastos_educacao', 'quantidade_matricula',
        'ideb_iniciais', 'ideb_finais',
        'taxa_abandono_ef_anos_iniciais', 'taxa_abandono_ef_anos_finais',
        'pib_per_capita', 'gasto_por_aluno'
    ]

        silver_df = add_completeness_flags(silver_df, value_columns)

        # Validate data
        if not validate_data(silver_df, schema=silver_schema, name="Silver"):
            raise ValueError("Silver data validation failed")
        
        # Data diagnostics
        analyze_data(silver_df, name="Silver")

        # Save data
        local_path = Path(paths["paths"]["silver"])
        layer = paths["layers"]["silver"]
        local_path.mkdir(parents=True, exist_ok=True)
        
        save.save_data(silver_df, "silver_data", directory=local_path)
        save.save_data_to_gcs(silver_df, "silver_data", bucket_name, layer=layer)
        
        print(f"Processing completed")
        logger.info(f"Data saved at {local_path} and GCP://{bucket_name}/{layer} successfully")
        return silver_df
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        return None

if __name__ == "__main__":
    silver_df = process_silver_data()