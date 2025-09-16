import basedosdados as bd
import os
from save import save_dataframe, save_dataframe_to_gcs
from dotenv import load_dotenv

# Access Key for basedosdados
load_dotenv()
bd.config.billing_project_id = os.getenv("billing_project_id")
bucket_name = os.getenv("gcp_bucket_name")


# População filtered DataFrame
query_df_pop = """
SELECT id_municipio, sigla_uf, ano, populacao
FROM `basedosdados.br_ibge_populacao.municipio`
WHERE ano IN (2017, 2019)
"""
bronze_df_pop = bd.read_sql(query_df_pop)


# PIB filtered DataFrame
query_df_pib = """
SELECT id_municipio, ano, pib
FROM `basedosdados.br_ibge_pib.municipio`
WHERE ano IN (2017, 2019)
"""
bronze_df_pib = bd.read_sql(query_df_pib)


# Gastos Educação filtered DataFrame
query_df_gastos_educ = """
SELECT id_municipio, sigla_uf, ano, valor
FROM `basedosdados.br_me_siconfi.municipio_despesas_funcao`
WHERE ano IN (2017, 2019)
AND estagio = "Despesas Pagas"
AND conta = "Educação"
"""
bronze_df_gastos_educ = bd.read_sql(query_df_gastos_educ)


# Matrículas filtered DataFrame
query_df_matriculas = """
SELECT id_municipio, sigla_uf, ano, SUM(quantidade_matricula) as quantidade_matricula
FROM `basedosdados.br_inep_sinopse_estatistica_educacao_basica.etapa_ensino_serie`
WHERE ano IN (2017, 2019)
AND rede = "Municipal"
AND etapa_ensino LIKE "Ensino Fundamental%" 
GROUP BY id_municipio, sigla_uf, ano
"""
bronze_df_matriculas = bd.read_sql(query_df_matriculas)


# Notas IDEB filtered DataFrame
query_df_ideb = """
SELECT id_municipio, sigla_uf, ano, anos_escolares, ideb
FROM `basedosdados.br_inep_ideb.municipio`
WHERE ano IN (2017, 2019)
AND ensino = "fundamental"
AND rede = "municipal"
"""
bronze_df_ideb = bd.read_sql(query_df_ideb)


# Taxas Abandono filtered DataFrame
query_df_abandono = """
SELECT id_municipio, ano, taxa_abandono_ef_anos_iniciais, taxa_abandono_ef_anos_finais
FROM `basedosdados.br_inep_indicadores_educacionais.municipio`
WHERE ano IN (2017, 2019)
AND rede = "municipal"
AND localizacao = "total"
"""
bronze_df_abandono = bd.read_sql(query_df_abandono)


# Dictionary to map DataFrame variable names to filenames
raw_dfs = {
    "bronze_df_pop": bronze_df_pop,
    "bronze_df_pib": bronze_df_pib,
    "bronze_df_gastos_educ": bronze_df_gastos_educ,
    "bronze_df_matriculas": bronze_df_matriculas,
    "bronze_df_ideb": bronze_df_ideb,
    "bronze_df_abandono": bronze_df_abandono
}

# Loop over dictionary using created function to save each DataFrame
for filename, df in raw_dfs.items():
    save_dataframe(df, filename)
    save_dataframe_to_gcs(df, filename, bucket_name, layer="bronze")