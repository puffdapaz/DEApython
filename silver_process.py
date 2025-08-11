import basedosdados as bd
import os
from save import save_dataframe
from dotenv import load_dotenv

# Access Key for basedosdados
load_dotenv()
bd.config.billing_project_id = os.getenv("billing_project_id")


# População filtered DataFrame
query_df_pop = """
SELECT 
  id_municipio, 
  sigla_uf, 
  ano, 
  CASE 
    WHEN SAFE_CAST(populacao AS INT64) = 0 THEN NULL
    WHEN SAFE_CAST(populacao AS INT64) < 0 THEN NULL
    ELSE SAFE_CAST(populacao AS INT64)
  END AS populacao
FROM `basedosdados.br_ibge_populacao.municipio`
WHERE ano IN (2017, 2019)
"""
try:
    silver_df_pop = bd.read_sql(query_df_pop)
except Exception as e:
    print(f"Error in population query: {str(e)}")
    silver_df_pop = None


# PIB filtered DataFrame
query_df_pib = """
SELECT id_municipio,
       ano,
  CASE
    WHEN SAFE_CAST(pib AS INT64) = 0 THEN NULL
    WHEN SAFE_CAST(pib AS INT64) < 0 THEN NULL
    ELSE SAFE_CAST(pib AS INT64)
  END AS pib
FROM `basedosdados.br_ibge_pib.municipio`
WHERE ano IN (2017, 2019)
"""
try:
    silver_df_pib = bd.read_sql(query_df_pib)
except Exception as e:
    print(f"Error in PIB query: {str(e)}")
    silver_df_pib = None


# Gastos Educação filtered DataFrame
query_df_gastos_educ = """
SELECT id_municipio,
       sigla_uf,
       ano,
  CASE
    WHEN SAFE_CAST(valor AS INT64) = 0 THEN NULL
    WHEN SAFE_CAST(valor AS INT64) < 0 THEN NULL
    ELSE SAFE_CAST(valor AS INT64)
  END AS valor
FROM `basedosdados.br_me_siconfi.municipio_despesas_funcao`
WHERE ano IN (2017, 2019)
AND estagio = "Despesas Pagas"
AND conta = "Educação"
"""
try:
    silver_df_gastos_educ = bd.read_sql(query_df_gastos_educ)
except Exception as e:
    print(f"Error in education expenses query: {str(e)}")
    silver_df_gastos_educ = None


# Matrículas filtered DataFrame
query_df_matriculas = """
WITH validated_data AS (
  SELECT
    id_municipio,
    sigla_uf,
    ano,
    CASE
      WHEN SAFE_CAST(quantidade_matricula AS INT64) IS NULL THEN NULL
      WHEN SAFE_CAST(quantidade_matricula AS INT64) < 0 THEN NULL
      ELSE SAFE_CAST(quantidade_matricula AS INT64)
    END AS validated_matricula
  FROM `basedosdados.br_inep_sinopse_estatistica_educacao_basica.etapa_ensino_serie`
  WHERE ano IN (2017, 2019)
    AND rede = "Municipal"
    AND etapa_ensino LIKE "Ensino Fundamental%"
)
SELECT id_municipio,
       sigla_uf,
       ano,
       SUM(validated_matricula) as quantidade_matricula
FROM validated_data
GROUP BY id_municipio, sigla_uf, ano
"""
try:
    silver_df_matriculas = bd.read_sql(query_df_matriculas)
except Exception as e:
    print(f"Error in enrollment query: {str(e)}")
    silver_df_matriculas = None


# Notas IDEB filtered DataFrame
query_df_ideb = """
WITH validated_data AS (
  SELECT
    id_municipio,
    sigla_uf,
    ano,
    anos_escolares,
    CASE
      WHEN SAFE_CAST(ideb AS FLOAT64) IS NULL THEN NULL
      WHEN SAFE_CAST(ideb AS FLOAT64) < 0 THEN NULL
      WHEN SAFE_CAST(ideb AS FLOAT64) > 10 THEN NULL
      ELSE SAFE_CAST(ideb AS FLOAT64)
    END AS validated_ideb
  FROM `basedosdados.br_inep_ideb.municipio`
  WHERE ano IN (2017, 2019)
    AND ensino = "fundamental"
    AND rede = "municipal"
)
SELECT
  id_municipio,
  sigla_uf,
  ano,
  MAX(CASE WHEN anos_escolares = 'iniciais (1-5)' THEN validated_ideb END) AS ideb_iniciais,
  MAX(CASE WHEN anos_escolares = 'finais (6-9)' THEN validated_ideb END) AS ideb_finais
FROM validated_data
GROUP BY id_municipio, sigla_uf, ano
"""
try:
    silver_df_ideb = bd.read_sql(query_df_ideb)
except Exception as e:
    print(f"Error in IDEB query: {str(e)}")
    silver_df_ideb = None


# Taxas Abandono filtered DataFrame
query_df_abandono = """
SELECT id_municipio, ano,
    CASE
      WHEN SAFE_CAST(taxa_abandono_ef_anos_iniciais AS FLOAT64) IS NULL THEN NULL
      WHEN SAFE_CAST(taxa_abandono_ef_anos_iniciais AS FLOAT64) < 0 THEN NULL
      ELSE SAFE_CAST(taxa_abandono_ef_anos_iniciais AS FLOAT64)
    END AS taxa_abandono_ef_anos_iniciais,
    CASE
      WHEN SAFE_CAST(taxa_abandono_ef_anos_finais AS FLOAT64) IS NULL THEN NULL
      WHEN SAFE_CAST(taxa_abandono_ef_anos_finais AS FLOAT64) < 0 THEN NULL
      ELSE SAFE_CAST(taxa_abandono_ef_anos_finais AS FLOAT64)
    END AS taxa_abandono_ef_anos_finais
FROM `basedosdados.br_inep_indicadores_educacionais.municipio`
WHERE ano IN (2017, 2019)
AND rede = "municipal"
AND localizacao = "total"
"""
try:
    silver_df_abandono = bd.read_sql(query_df_abandono)
except Exception as e:
    print(f"Error in abandonment rates query: {str(e)}")
    silver_df_abandono = None


# Dictionary to map DataFrame variable names to filenames
silver_dfs = {
    "silver_df_pop": silver_df_pop,
    "silver_df_pib": silver_df_pib,
    "silver_df_gastos_educ": silver_df_gastos_educ,
    "silver_df_matriculas": silver_df_matriculas,
    "silver_df_ideb": silver_df_ideb,
    "silver_df_abandono": silver_df_abandono
}

# Loop over dictionary using created function to save each DataFrame
for filename, df in silver_dfs.items():
    save_dataframe(df, filename, directory="data/processed/silver", file_format="csv")