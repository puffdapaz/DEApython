import basedosdados as bd
import os
import yaml
import logging
from .save_utils import save as save
from typing import Dict, Optional
from pathlib import Path
from dotenv import load_dotenv
from .diagnostics.data_validation import schemas, validate_data

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
        raise ValueError("Missing required environment variables: billing_project_id or gcp_bucket_name")
    
    bd.config.billing_project_id = billing_project_id
    
    return bucket_name

def load_bronze_data(layer: str = "bronze") -> Dict[str, str]:
    """
    Load queries from YAML configuration file.
    Args:
        layer: Data layer ('bronze', 'silver', 'gold')
    Returns:
        Dictionary of query names to SQL strings
    """
    try:
        with open("configs/queries.yml", "r", encoding="utf-8") as f:
            all_queries = yaml.safe_load(f)

        layer_queries = all_queries.get(f"{layer}_queries", {})
        
        if not layer_queries:
            logger.warning(f"No queries found for layer: {layer}")
            return {}
        return layer_queries
    
    except Exception as e:
        logger.error(f"Unexpected error loading queries: {e}")
        raise

def bronze_ingestion():
    """Main function for bronze layer data ingestion."""
    try:
        # Load configurations
        dea_config, paths = load_configs()
        
        # Set up Base dos Dados
        bucket_name = setup_basedosdados()
        
        # Load queries directly from config
        queries = load_bronze_data("bronze")
        
        if not queries:
            raise ValueError("No bronze queries found in configuration")
        
        # Load all data
        dataframes = {}
        for query_name, query in queries.items():
            try:
                df = bd.read_sql(query, billing_project_id=os.getenv("billing_project_id"))
                dataframes[query_name] = df
            except Exception as e:
                logger.error(f"Error running query {query_name}: {e}")
        
        # Validate data
        if not validate_data(dataframes, schema_map=schemas):
            raise ValueError("Bronze data validation failed")
        
        # Save data
        local_path = Path(paths["paths"]["bronze"])
        layer = paths["layers"]["bronze"]
        local_path.mkdir(parents=True, exist_ok=True)

        for filename, df in dataframes.items():
            if df is not None:
                try:
                    save.save_data(df, f"bronze_{filename}", directory=local_path)
                    save.save_data_to_gcs(df, f"bronze_{filename}", bucket_name, layer=layer)
                except Exception as e:
                    logger.error(f"Error saving {filename}: {e}")
        
        print(f"Ingestion completed")
        logger.info(f"Data saved at {local_path} and GCP://{bucket_name}/{layer} successfully")
        return dataframes
        
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise

if __name__ == "__main__":
    bronze_ingestion()