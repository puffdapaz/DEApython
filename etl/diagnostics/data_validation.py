import pandas as pd
import basedosdados as bd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
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

# Define schemas for bronze datasets
population_schema = DataFrameSchema({
    "id_municipio": Column(str),
    "sigla_uf": Column(str, nullable=True),
    "ano": Column(int, checks=Check.isin([2017, 2019])),
    "populacao": Column(int, checks=Check.ge(0))
})

pib_schema = DataFrameSchema({
    "id_municipio": Column(str),
    "ano": Column(int, checks=Check.isin([2017, 2019])),
    "pib": Column(float, checks=Check.ge(0))
})

education_spending_schema = DataFrameSchema({
    "id_municipio": Column(str),
    "sigla_uf": Column(str, nullable=True),
    "ano": Column(int, checks=Check.isin([2017, 2019])),
    "valor": Column(float, checks=Check.ge(0))
})

enrollments_schema = DataFrameSchema({
    "id_municipio": Column(str),
    "sigla_uf": Column(str, nullable=True),
    "ano": Column(int, checks=Check.isin([2017, 2019])),
    "quantidade_matricula": Column(int, checks=Check.ge(0))
})

ideb_schema = DataFrameSchema({
    "id_municipio": Column(str),
    "sigla_uf": Column(str, nullable=True),
    "ano": Column(int, checks=Check.isin([2017, 2019])),
    "anos_escolares": Column(str, nullable=True, checks=Check.isin(["iniciais (1-5)", "finais (6-9)"])),
    "ideb": Column(float, checks=Check.between(0, 10))
})

dropout_rates_schema = DataFrameSchema({
    "id_municipio": Column(str),
    "ano": Column(int, checks=Check.isin([2017, 2019])),
    "taxa_abandono_ef_anos_iniciais": Column(float, checks=Check.between(0, 100)),
    "taxa_abandono_ef_anos_finais": Column(float, checks=Check.between(0, 100))
})

schemas = {
    "population": population_schema,
    "pib": pib_schema,
    "education_spending": education_spending_schema,
    "enrollments": enrollments_schema,
    "ideb": ideb_schema,
    "dropout_rates": dropout_rates_schema,
}

def validate_bronze_data(dataframes: Dict[str, bd.Table]) -> bool:
    """Validate that all required bronze data was loaded successfully."""
    all_valid = True
    for name, df in dataframes.items():
        if df is not None:
            try:
                if name in schemas:
                    schemas[name].validate(df, lazy=True)
                    logger.info(f"✅ {name} validation passed")
                else:
                    logger.warning(f"No schema defined for {name}, skipping validation")
            except pa.errors.SchemaErrors as e:
                logger.error(f"❌ {name} validation failed:\n{e.failure_cases}")
                all_valid = False
    return all_valid

silver_schema = DataFrameSchema({
    "id_municipio": Column(str, nullable=False),
    "sigla_uf": Column(str, nullable=True),
    "ano": Column(int, checks=Check.isin([2017, 2019])),
    "populacao": Column(int, nullable=True, checks=Check.ge(0)),
    "nome": Column(str, nullable=True),
    "pib": Column(float, nullable=True, checks=Check.ge(0)),
    "gastos_educacao": Column(float, nullable=True, checks=Check.ge(0)),
    "quantidade_matricula": Column(int, nullable=True, checks=Check.ge(0)),
    "ideb_iniciais": Column(float, nullable=True, checks=Check.between(0, 10)),
    "ideb_finais": Column(float, nullable=True, checks=Check.between(0, 10)),
    "taxa_abandono_ef_anos_iniciais": Column(float, nullable=True, checks=Check.between(0, 100)),
    "taxa_abandono_ef_anos_finais": Column(float, nullable=True, checks=Check.between(0, 100)),
    "pib_per_capita": Column(float, nullable=True, checks=Check.ge(0)),
    "gasto_por_aluno": Column(float, nullable=True, checks=Check.ge(0)),
    "is_complete_grouped": Column(bool, nullable=True),
})

def validate_silver_data(df) -> bool:
    """Validate silver data quality."""
    try:
        silver_schema.validate(df, lazy=True)
        logger.info("✅ Silver data validation passed")
        return True
    except pa.errors.SchemaErrors as e:
        logger.error(f"❌ Silver data validation failed:\n{e.failure_cases}")
        return False

gold_schema = DataFrameSchema({
    # Keys
    "id_municipio": Column(str, nullable=False),
    "ano": Column(int, nullable=False, checks=Check.isin([2017, 2019])),

    # Original silver features
    "pib_per_capita": Column(float, nullable=True, checks=Check.ge(0)),
    "gasto_por_aluno": Column(float, nullable=True, checks=Check.ge(0)),
    "ideb_iniciais": Column(float, nullable=True, checks=Check.between(0, 10)),
    "ideb_finais": Column(float, nullable=True, checks=Check.between(0, 10)),
    "taxa_abandono_ef_anos_iniciais": Column(float, nullable=True, checks=Check.between(0, 100)),
    "taxa_abandono_ef_anos_finais": Column(float, nullable=True, checks=Check.between(0, 100)),

    # DEA efficiency metrics
    "DEA_crs_input": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_crs_output": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_vrs_input": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_vrs_output": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_irs_input": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_drs_input": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_scale_efficiency": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_returns_nature": Column(str, nullable=True, checks=Check.isin(["Constante", "Crescente", "Decrescente"])),
})


def validate_gold_data(gold_df) -> bool:
    """Validate silver data quality."""
    try:
        gold_schema.validate(df, lazy=True)
        logger.info("✅ Gold data validation passed")
        return True
    except pa.errors.SchemaErrors as e:
        logger.error(f"❌ Gold data validation failed:\n{e.failure_cases}")
        return False
