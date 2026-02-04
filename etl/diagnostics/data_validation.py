"""
Data validation schemas and utilities using Pandera.

This module defines data quality schemas for different data layers:
- Bronze: Raw source data schemas
- Silver: Enriched, joined data schemas  
- Gold: Final feature schemas for modeling/analysis

Also provides validation functions to enforce data quality standards.
"""
import pandas as pd
import pandera as pa
import logging
from typing import Optional
from pandera import Column, DataFrameSchema, Check

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------
VALID_YEARS: list[int] = [2017, 2019]

# ---------------------------------------------------------------------
# Bronze layer schemas (raw data)
# ---------------------------------------------------------------------
population_schema = DataFrameSchema({
    "id_municipio": Column(str),
    "ano": Column(int, checks=Check.isin(VALID_YEARS)),
    "populacao": Column(int, checks=Check.ge(0))
})

gdp_schema = DataFrameSchema({
    "id_municipio": Column(str),
    "ano": Column(int, checks=Check.isin(VALID_YEARS)),
    "pib": Column(int, checks=Check.ge(0))
})

education_spending_schema = DataFrameSchema({
    "id_municipio": Column(str),
    "ano": Column(int, checks=Check.isin(VALID_YEARS)),
    "valor": Column(float, checks=Check.ge(0))
})

enrollments_schema = DataFrameSchema({
    "id_municipio": Column(str),
    "ano": Column(int, checks=Check.isin(VALID_YEARS)),
    "quantidade_matricula": Column(int, checks=Check.ge(0))
})

ideb_schema = DataFrameSchema({
    "id_municipio": Column(str),
    "ano": Column(int, checks=Check.isin(VALID_YEARS)),
    "anos_escolares": Column(str, nullable=True, checks=Check.isin(["iniciais (1-5)", "finais (6-9)"])),
    "ideb": Column(float, nullable=True, checks=Check.between(0, 10))
})

dropout_rates_schema = DataFrameSchema({
    "id_municipio": Column(str),
    "ano": Column(int, checks=Check.isin(VALID_YEARS)),
    "taxa_abandono_ef_anos_iniciais": Column(float, nullable=True, checks=Check.between(0, 100)),
    "taxa_abandono_ef_anos_finais": Column(float, nullable=True, checks=Check.between(0, 100))
})

# Dictionary of bronze schemas
schemas = {
    "population": population_schema,
    "gdp": gdp_schema,
    "education_spending": education_spending_schema,
    "enrollments": enrollments_schema,
    "ideb": ideb_schema,
    "dropout_rates": dropout_rates_schema,
}

# ---------------------------------------------------------------------
# Silver layer schema (enriched, joined data)
# ---------------------------------------------------------------------
silver_schema = DataFrameSchema({
    "city_id": Column(str, nullable=False),
    "year": Column(int, checks=Check.isin(VALID_YEARS)),
    "population": Column(int, nullable=True, checks=Check.ge(0)),
    "city_name": Column(str, nullable=True),
    "state_id": Column(str, nullable=False),
    "state_name": Column(str, nullable=False),
    "gdp": Column(int, nullable=True, checks=Check.ge(0)),
    "education_spending": Column(int, nullable=True, checks=Check.ge(0)),
    "enrollments": Column(int, nullable=True, checks=Check.ge(0)),
    "ideb_initial_years": Column(float, nullable=True, checks=Check.between(0, 10)),
    "ideb_final_years": Column(float, nullable=True, checks=Check.between(0, 10)),
    "dropout_rates_initial_years": Column(float, nullable=True, checks=Check.between(0, 100)),
    "dropout_rates_final_years": Column(float, nullable=True, checks=Check.between(0, 100)),
    "gdp_per_capita": Column(float, nullable=True, checks=Check.ge(0)),
    "spending_per_student": Column(float, nullable=True, checks=Check.ge(0)),
    "educ_pct_gdp": Column(float, nullable=True, checks=Check.between(0, 100)),
    "is_complete_grouped": Column(bool, nullable=False),
})

# ---------------------------------------------------------------------
# Gold layer schema (final features for modeling/DEA)
# ---------------------------------------------------------------------
gold_schema = DataFrameSchema({
    "city_id": Column(str, nullable=False),
    "year": Column(int, checks=Check.isin(VALID_YEARS)),
    "population": Column(int, nullable=True, checks=Check.ge(0)),
    "city_name": Column(str, nullable=True),
    "state_id": Column(str, nullable=False),
    "state_name": Column(str, nullable=False),
    "gdp": Column(int, nullable=True, checks=Check.ge(0)),
    "education_spending": Column(int, nullable=True, checks=Check.ge(0)),
    "enrollments": Column(int, nullable=True, checks=Check.ge(0)),
    "ideb_initial_years": Column(float, nullable=True, checks=Check.between(0, 10)),
    "ideb_final_years": Column(float, nullable=True, checks=Check.between(0, 10)),
    "dropout_rates_initial_years": Column(float, nullable=True, checks=Check.between(0, 100)),
    "dropout_rates_final_years": Column(float, nullable=True, checks=Check.between(0, 100)),
    "gdp_per_capita": Column(float, nullable=True, checks=Check.ge(0)),
    "spending_per_student": Column(float, nullable=True, checks=Check.ge(0)),
    "educ_pct_gdp": Column(float, nullable=True, checks=Check.between(0, 1)),
    # "geometry": Column(object, nullable=True), Special Object type for geometries
    "is_complete_grouped": Column(bool, nullable=False),
    "DEA_crs_input": Column(float, nullable=True, checks=Check.between(0, 1)),
    # "DEA_crs_output": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_vrs_input": Column(float, nullable=True, checks=Check.between(0, 1)),
    # "DEA_vrs_output": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_irs_input": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_drs_input": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_scale_efficiency": Column(float, nullable=True, checks=Check.between(0, 1)),
    "DEA_returns_nature": Column(str, nullable=True, checks=Check.isin(["Constant", "Increasing", "Decreasing"])),
    "national_median_vrs": Column(float, nullable=True, checks=Check.between(0, 1)),
    "state_avg_vrs": Column(float, nullable=True, checks=Check.between(0, 1)),
    "rank_vrs": Column(int, nullable=True, checks=Check.between(0, 5570)),
    "state_rank_vrs": Column(int, nullable=True, checks=Check.between(0, 860)),
})

# ---------------------------------------------------------------------
# Validation function
# ---------------------------------------------------------------------
def validate_data(data: pd.DataFrame | dict[str, pd.DataFrame],
                  schema_map: Optional[dict[str, pa.DataFrameSchema]] = None,
                  schema: Optional[pa.DataFrameSchema] = None,
                  name: str = "dataset") -> bool:
    """
    Validate input data against defined Pandera schemas.
    Args:
        data: Either a single DataFrame or a dict of named DataFrames.
    schema_map: Mapping of dataset names to schemas (used if `data` is a dict).
    schema: Schema for single DataFrame validation.
    name: Dataset name used for logging messages.
    Returns:
        bool: True if all validations pass, False otherwise.
    Raises:
        ValueError: If schema is missing for single DataFrame validation.
        TypeError: If `data` is neither DataFrame nor dict.
    """
    if isinstance(data,
                  dict):
        all_valid = True
        for name, df in data.items():
            if df is None:
                logger.warning(f"{name} DataFrame is None, skipping.")
                continue
            if schema_map and name in schema_map:
                try:
                    schema_map[name].validate(df,
                                              lazy=True)
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
            schema.validate(data,
                            lazy=True)
            print(f"{name} validation passed. Shape: {data.shape}")
            return True
        except pa.errors.SchemaErrors as e:
            logger.error(f"{name} validation failed:\n{e.failure_cases}")
            return False
    else:
        raise TypeError("data must be either a DataFrame or a dict of DataFrames")