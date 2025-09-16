import pandas as pd
import basedosdados as bd
import numpy as np
import yaml
from typing import Dict, List, Any
import logging

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

def validate_bronze_data(dataframes: Dict[str, bd.Table]) -> bool:
    """Validate that all required bronze data was loaded successfully."""
    required_tables = ["population", "pib", "education_spending", "enrollments", "ideb", "dropout_rates"]
    
    for table_name in required_tables:
        if table_name not in dataframes or dataframes[table_name] is None:
            logger.error(f"Missing required table: {table_name}")
            return False
        if dataframes[table_name].empty:
            logger.error(f"Empty table: {table_name}")
            return False
    
    logger.info("All bronze data validated successfully")
    return True

def validate_silver_data(df) -> bool:
    """Validate silver data quality."""
    if df is None or df.empty:
        logger.error("Silver data is empty or None")
        return False
    
    required_columns = [
        'id_municipio', 'sigla_uf', 'ano', 'nome', 'populacao',
        'pib', 'gastos_educacao', 'quantidade_matricula', 'ideb_iniciais',
        'ideb_finais', 'taxa_abandono_ef_anos_iniciais', 'taxa_abandono_ef_anos_finais',
        'pib_per_capita', 'gasto_por_aluno'
    ]
    
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    
    # Check for reasonable data ranges
    validation_checks = {
        'populacao': (lambda x: x > 0, "Population should be positive"),
        'pib': (lambda x: x > 0, "GDP should be positive"),
        'gastos_educacao': (lambda x: x > 0, "Education spending should be positive"),
        'quantidade_matricula': (lambda x: x > 0, "Enrollments should be positive"),
        'ideb_iniciais': (lambda x: 0 <= x <= 10, "IDEB should be between 0-10"),
        'ideb_finais': (lambda x: 0 <= x <= 10, "IDEB should be between 0-10"),
        'taxa_abandono_ef_anos_iniciais': (lambda x: 0 <= x <= 100, "Dropout rate should be 0-100%"),
        'taxa_abandono_ef_anos_finais': (lambda x: 0 <= x <= 100, "Dropout rate should be 0-100%")
    }
    
    for col, (check_func, message) in validation_checks.items():
        if col in df.columns:
            invalid_count = df[df[col].notna() & ~df[col].apply(check_func)].shape[0]
            if invalid_count > 0:
                logger.warning(f"{invalid_count} records with invalid {col}: {message}")
    
    logger.info(f"Silver data validation passed. Shape: {df.shape}")
    return True

def validate_gold_data(gold_df) -> bool:
    """Validate silver data quality."""
    if gold_df is None or gold_df.empty:
        logger.error("Silver data is empty or None")
        return False
    
    required_columns = [
        'DEA_crs_input', 'DEA_crs_output', 'DEA_vrs_input', 'DEA_vrs_output',
        'DEA_irs_input', 'DEA_drs_input', 'DEA_scale_efficiency', 'DEA_returns_nature'
    ]
    
    missing_columns = set(required_columns) - set(gold_df.columns)
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    
    # Check for reasonable data ranges
    validation_checks = {
        'DEA_crs_input': (lambda x: 0 <= x <= 1, "DEA interval should be 0-1"),
        'DEA_crs_output': (lambda x: 0 <= x <= 1, "DEA interval should be 0-1"),
        'DEA_vrs_input': (lambda x: 0 <= x <= 1, "DEA interval should be 0-1"),
        'DEA_vrs_output': (lambda x: 0 <= x <= 1, "DEA interval should be 0-1"),
        'DEA_irs_input': (lambda x: 0 <= x <= 1, "DEA interval should be 0-1"),
        'DEA_drs_input': (lambda x: 0 <= x <= 1, "DEA interval should be 0-1"),
        'DEA_scale_efficiency': (lambda x: 0 <= x <= 1, "DEA interval should be 0-1")
    }
    
    for col, (check_func, message) in validation_checks.items():
        if col in gold_df.columns:
            invalid_count = gold_df[gold_df[col].notna() & ~gold_df[col].apply(check_func)].shape[0]
            if invalid_count > 0:
                logger.warning(f"{invalid_count} records with invalid {col}: {message}")
    
    logger.info(f"Gold data validation passed. Shape: {gold_df.shape}")
    return True