"""
This script reads the full dataframes
(fields, occurrencies and periods) to filter the tables used.
Requires excellent hardware to run.
"""

import basedosdados as bd
import pandas as pd
import os
from save import save_dataframe
from dotenv import load_dotenv


# Access Key for basedosdados
load_dotenv()
bd.config.billing_project_id = os.getenv("billing_project_id")


# População DataFrame
df_pop = bd.read_table(
    dataset_id="br_ibge_populacao",
    table_id="municipio"
)

# PIB DataFrame
df_pib = bd.read_table(
    dataset_id="br_ibge_pib",
    table_id="municipio"
)

# Contas Nacionais DataFrame
df_gastos_func = bd.read_table(
    dataset_id="br_me_siconfi",
    table_id="municipio_despesas_funcao"
)

# Sinopse Educação Basica DataFrame
df_ensino_serie = bd.read_table(
    dataset_id="br_inep_sinopse_estatistica_educacao_basica",
    table_id="etapa_ensino_serie"
)

# INEP IDEB DataFrame
df_inep_ideb = bd.read_table(
    dataset_id="br_inep_ideb",
    table_id="municipio"
)

# Indicadores Educacionais DataFrame
df_indic_educ = bd.read_table(
    dataset_id="br_inep_indicadores_educacionais",
    table_id="municipio"
)

# Dictionary to map DataFrame variable names to filenames
raw_dfs = {
    "df_pop": df_pop,
    "df_pib": df_pib,
    "df_gastos_func": df_gastos_func,
    "df_ensino_serie": df_ensino_serie,
    "df_inep_ideb": df_inep_ideb,
    "df_indic_educ": df_indic_educ
}

# Loop over dictionary using created function to save each DataFrame
for filename, df in raw_dfs.items():
    save_dataframe(df, filename, file_format="parquet")