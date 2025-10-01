import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
import yaml
from typing import Union, Dict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_configs() -> tuple:
    """Load configurations from YAML files with proper type hints"""
    config_files = {
        "dea_config": "configs/dea_config.yml",
        "paths": "configs/path.yml"
    }
    configs = {}
    
    for name, filepath in config_files.items():
        try:
            with open(filepath, "r") as f:
                configs[name] = yaml.safe_load(f)
        except FileNotFoundError as e:
            logger.error(f"Configuration file not found: {filepath}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML config {filepath}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error loading {filepath}: {e}")
            raise
    return configs["dea_config"], configs["paths"]

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
    "pib": Column(int, checks=Check.ge(0))
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
    "ideb": Column(float, nullable=True, checks=Check.between(0, 10))
})

dropout_rates_schema = DataFrameSchema({
    "id_municipio": Column(str),
    "ano": Column(int, checks=Check.isin([2017, 2019])),
    "taxa_abandono_ef_anos_iniciais": Column(float, nullable=True, checks=Check.between(0, 100)),
    "taxa_abandono_ef_anos_finais": Column(float, nullable=True, checks=Check.between(0, 100))
})

schemas = {
    "population": population_schema,
    "pib": pib_schema,
    "education_spending": education_spending_schema,
    "enrollments": enrollments_schema,
    "ideb": ideb_schema,
    "dropout_rates": dropout_rates_schema,
}

silver_schema = DataFrameSchema({
    "id_municipio": Column(str, nullable=False),
    "sigla_uf": Column(str, nullable=False),
    "ano": Column(int, checks=Check.isin([2017, 2019])),
    "populacao": Column(int, nullable=True, checks=Check.ge(0)),
    "nome": Column(str, nullable=True),
    "pib": Column(int, nullable=True, checks=Check.ge(0)),
    "gastos_educacao": Column(int, nullable=True, checks=Check.ge(0)),
    "quantidade_matricula": Column(int, nullable=True, checks=Check.ge(0)),
    "ideb_iniciais": Column(float, nullable=True, checks=Check.between(0, 10)),
    "ideb_finais": Column(float, nullable=True, checks=Check.between(0, 10)),
    "taxa_abandono_ef_anos_iniciais": Column(float, nullable=True, checks=Check.between(0, 100)),
    "taxa_abandono_ef_anos_finais": Column(float, nullable=True, checks=Check.between(0, 100)),
    "pib_per_capita": Column(float, nullable=True, checks=Check.ge(0)),
    "gasto_por_aluno": Column(float, nullable=True, checks=Check.ge(0)),
    "is_complete_grouped": Column(bool, nullable=False),
})

gold_schema = DataFrameSchema({
    "id_municipio": Column(str, nullable=False),
    "ano": Column(int, nullable=False, checks=Check.isin([2017, 2019])),
    "pib_per_capita": Column(float, nullable=True, checks=Check.ge(0)),
    "gasto_por_aluno": Column(float, nullable=True, checks=Check.ge(0)),
    "ideb_iniciais": Column(float, nullable=True, checks=Check.between(0, 10)),
    "ideb_finais": Column(float, nullable=True, checks=Check.between(0, 10)),
    "taxa_abandono_ef_anos_iniciais": Column(float, nullable=True, checks=Check.between(0, 100)),
    "taxa_abandono_ef_anos_finais": Column(float, nullable=True, checks=Check.between(0, 100)),
    "DEA_crs_input": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_crs_output": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_vrs_input": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_vrs_output": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_irs_input": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_drs_input": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_scale_efficiency": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_returns_nature": Column(str, nullable=True, checks=Check.isin(["Constante", "Crescente", "Decrescente"])),
})

def validate_data(
                data: Union[pd.DataFrame, Dict[str, pd.DataFrame]],
                schema_map: Dict[str, pa.DataFrameSchema] = None,
                schema: pa.DataFrameSchema = None,
                name: str = "dataset"
) -> bool:
    """
    Validate either:
      - a single DataFrame against a given schema
      - a dict of DataFrames against schema_map
    """
    if isinstance(data, dict):
        all_valid = True
        for name, df in data.items():
            if df is None:
                logger.warning(f"{name} DataFrame is None, skipping.")
                continue
            if schema_map and name in schema_map:
                try:
                    schema_map[name].validate(df, lazy=True)
                    print(f"{name} validation passed. Shape: {df.shape}")
                except pa.errors.SchemaErrors as e:
                    logger.error(f"{name} validation failed:\n{e.failure_cases}")
                    all_valid = False
            else:
                logger.warning(f"No schema defined for {name}, skipping.")
        return all_valid

    elif isinstance(data, pd.DataFrame):
        if schema is None:
            raise ValueError("A schema must be provided for single DataFrame validation")
        try:
            schema.validate(data, lazy=True)
            print(f"{name} validation passed. Shape: {data.shape}")
            return True
        except pa.errors.SchemaErrors as e:
            logger.error(f"{name} validation failed:\n{e.failure_cases}")
            return False

    else:
        raise TypeError("data must be either a DataFrame or a dict of DataFrames")