import basedosdados as bd
import os
import yaml
import logging
from typing import Dict, List, Optional
from pathlib import Path
from dotenv import load_dotenv
from .save_utils.save import save_dataframe, save_dataframe_to_gcs
from .diagnostics.data_validation import validate_bronze_data

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

def load_bronze_data(query: str, query_name: str) -> Optional[bd.Table]:
    """Load data from Base dos Dados with error handling."""
    try:
        df = bd.read_sql(query)
        return df
    except Exception as e:
        logger.error(f"Error loading {query_name}: {e}")
        return None

def get_bronze_queries() -> Dict[str, str]:
    """Return all bronze layer SQL queries."""
    return {
        "population": """
            SELECT id_municipio, sigla_uf, ano, populacao
            FROM `basedosdados.br_ibge_populacao.municipio`
            WHERE ano IN (2017, 2019)
        """,
        "pib": """
            SELECT id_municipio, ano, pib
            FROM `basedosdados.br_ibge_pib.municipio`
            WHERE ano IN (2017, 2019)
        """,
        "education_spending": """
            SELECT id_municipio, sigla_uf, ano, valor
            FROM `basedosdados.br_me_siconfi.municipio_despesas_funcao`
            WHERE ano IN (2017, 2019)
            AND estagio = "Despesas Pagas"
            AND conta = "Educação"
        """,
        "enrollments": """
            SELECT id_municipio, sigla_uf, ano, SUM(quantidade_matricula) as quantidade_matricula
            FROM `basedosdados.br_inep_sinopse_estatistica_educacao_basica.etapa_ensino_serie`
            WHERE ano IN (2017, 2019)
            AND rede = "Municipal"
            AND etapa_ensino LIKE "Ensino Fundamental%" 
            GROUP BY id_municipio, sigla_uf, ano
        """,
        "ideb": """
            SELECT id_municipio, sigla_uf, ano, anos_escolares, ideb
            FROM `basedosdados.br_inep_ideb.municipio`
            WHERE ano IN (2017, 2019)
            AND ensino = "fundamental"
            AND rede = "municipal"
        """,
        "dropout_rates": """
            SELECT id_municipio, ano, taxa_abandono_ef_anos_iniciais, taxa_abandono_ef_anos_finais
            FROM `basedosdados.br_inep_indicadores_educacionais.municipio`
            WHERE ano IN (2017, 2019)
            AND rede = "municipal"
            AND localizacao = "total"
        """
    }

def bronze_ingestion():
    """Main function for bronze layer data ingestion."""
    
    try:
        # Load configurations
        dea_config, paths = load_configs()
        
        # Set up Base dos Dados
        bucket_name = setup_basedosdados()
        
        # Get all queries
        queries = get_bronze_queries()
        
        # Load all data
        dataframes = {}
        for query_name, query in queries.items():
            df = load_bronze_data(query, query_name)
            dataframes[query_name] = df
        
        # Validate data
        if not validate_bronze_data(dataframes):
            raise ValueError("Bronze data validation failed")
        
        # Save data
        local_path = Path("data/raw")
        layer = "bronze"
        local_path.mkdir(parents=True, exist_ok=True)

        for filename, df in dataframes.items():
                if df is not None:
                    try:
                        # Save locally
                        save_dataframe(df, f"bronze_{filename}", directory=local_path)
                        
                        # Save to GCS
                        save_dataframe_to_gcs(df, f"bronze_{filename}", bucket_name, layer="bronze")
                        
                    except Exception as e:
                        logger.error(f"Error saving {filename}: {e}")
        
        logger.info(f"Ingestion completed")
        logger.info(f"Data saved at {local_path} and GCP://{bucket_name}/{layer} successfully")
        return dataframes
        
    except Exception as e:
        logger.error(f"Bronze ingestion failed: {e}")
        raise

if __name__ == "__main__":
    bronze_ingestion()