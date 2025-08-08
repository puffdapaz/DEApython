import basedosdados as bd
import pandas as pd
import os
from save import save_dataframe
from dotenv import load_dotenv

# Access Key for basedosdados
load_dotenv()
bd.config.billing_project_id = os.getenv("billing_project_id")


# # População DataFrame
# df_pop = bd.read_table(
#     dataset_id="br_ibge_populacao",
#     table_id="municipio"
# )
# print(df_pop.head)

# População filtered DataFrame
query_df_pop = """
SELECT id_municipio, sigla_uf, ano, populacao
FROM `basedosdados.br_ibge_populacao.municipio`
WHERE ano IN (2017, 2019, 2023)
"""

silver_df_pop = bd.read_sql(query_df_pop)
save_dataframe(silver_df_pop, 'silver_df_pop', file_format="csv")

# # PIB DataFrame
# df_pib = bd.read_table(
#     dataset_id="br_ibge_pib",
#     table_id="municipio"
# )
# print(df_pib.head)

# PIB filtered DataFrame
query_df_pib = """
SELECT id_municipio, ano, pib
FROM `basedosdados.br_ibge_pib.municipio`
WHERE ano IN (2017, 2019, 2023)
"""

silver_df_pib = bd.read_sql(query_df_pib)
save_dataframe(silver_df_pib, 'silver_df_pib', file_format="csv")

# # Contas Nacionais DataFrame
# df_gastos_func = bd.read_table(
#     dataset_id="br_me_siconfi",
#     table_id="municipio_despesas_funcao"
# )
# print(df_gastos_func.head)

# Gastos Educação filtered DataFrame
query_df_gastos_educ = """
SELECT id_municipio, sigla_uf, ano, valor
FROM `basedosdados.br_me_siconfi.municipio_despesas_funcao`
WHERE ano IN (2017, 2019, 2023)
AND estagio = "Despesas Pagas"
AND conta = "Educação"
"""

silver_df_gastos_educ = bd.read_sql(query_df_gastos_educ)
save_dataframe(silver_df_gastos_educ, 'silver_df_gastos_educ', file_format="csv")

# # Sinopse Educação Basica DataFrame
# df_ensino_serie = bd.read_table(
#     dataset_id="br_inep_sinopse_estatistica_educacao_basica",
#     table_id="etapa_ensino_serie"
# )
# print(df_ensino_serie.head)

# Matrículas filtered DataFrame
query_df_matriculas = """
SELECT id_municipio, sigla_uf, ano, SUM(quantidade_matricula)
FROM `basedosdados.br_inep_sinopse_estatistica_educacao_basica.etapa_ensino_serie`
WHERE ano IN (2017, 2019, 2023)
AND rede = "Municipal"
AND etapa_ensino LIKE "Ensino Fundamental%" 
GROUP BY id_municipio, sigla_uf, ano
"""

silver_df_matriculas = bd.read_sql(query_df_matriculas)
save_dataframe(silver_df_matriculas, 'silver_df_matriculas', file_format="csv")

# # INEP IDEB DataFrame
# df_inep_ideb = bd.read_table(
#     dataset_id="br_inep_ideb",
#     table_id="municipio"
# )
# print(df_inep_ideb.head)

# Notas IDEB filtered DataFrame
query_df_ideb = """
SELECT id_municipio, sigla_uf, ano, anos_escolares, ideb
FROM `basedosdados.br_inep_ideb.municipio`
WHERE ano IN (2017, 2019, 2023)
AND ensino = "fundamental"
AND rede = "municipal"
"""

silver_df_ideb = bd.read_sql(query_df_ideb)
save_dataframe(silver_df_ideb, 'silver_df_ideb', file_format="csv")

# # Indicadores Educacionais DataFrame
# df_indic_educ = bd.read_table(
#     dataset_id="br_inep_indicadores_educacionais",
#     table_id="municipio"
# )
# print(df_indic_educ.head)

# Taxas Abandono filtered DataFrame
query_df_abandono = """
SELECT id_municipio, ano, taxa_abandono_ef_anos_iniciais, taxa_abandono_ef_anos_finais
FROM `basedosdados.br_inep_indicadores_educacionais.municipio`
WHERE ano IN (2017, 2019, 2023)
AND rede = "municipal"
AND localizacao = "total"
"""

silver_df_abandono = bd.read_sql(query_df_abandono)
save_dataframe(silver_df_abandono, 'silver_df_abandono', file_format="csv")

# # Dictionary to map DataFrame variable names to filenames
# raw_dfs = {
#     "silver_df_pop": silver_df_pop,
#     "silver_df_pib": silver_df_pib,
#     "silver_df_gastos_educ": silver_df_gastos_educ,
#     "silver_df_matriculas": silver_df_matriculas,
#     "silver_df_ideb": silver_df_ideb,
#     "silver_df_abandono": silver_df_abandono
# }

# # Loop over dictionary using created function to save each DataFrame
# for filename, df in raw_dfs.items():
#     save_dataframe(df, filename, file_format="parquet")