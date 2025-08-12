import basedosdados as bd
import os
from save import save_dataframe
from dotenv import load_dotenv

# Access Key for basedosdados
load_dotenv()
bd.config.billing_project_id = os.getenv("billing_project_id")

silver_df = """
WITH 
-- Population data
populacao AS (
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
),

-- names data
name AS (
  SELECT 
    id_municipio,
    nome
  FROM `basedosdados.br_bd_diretorios_brasil.municipio`
),

-- GDP data
pib AS (
  SELECT 
    id_municipio,
    ano,
    CASE
      WHEN SAFE_CAST(pib AS INT64) = 0 THEN NULL
      WHEN SAFE_CAST(pib AS INT64) < 0 THEN NULL
      ELSE SAFE_CAST(pib AS INT64)
    END AS pib
  FROM `basedosdados.br_ibge_pib.municipio`
  WHERE ano IN (2017, 2019)
),

-- Education expenses
gastos_educ AS (
  SELECT 
    id_municipio,
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
),

-- Enrollments
matriculas AS (
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
  SELECT 
    id_municipio,
    sigla_uf,
    ano,
    SUM(validated_matricula) as quantidade_matricula
  FROM validated_data
  GROUP BY id_municipio, sigla_uf, ano
),

-- IDEB scores
ideb AS (
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
),

-- Abandonment rates
abandono AS (
  SELECT 
    id_municipio, 
    ano,
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
)

-- Final combined query
SELECT 
  p.id_municipio,
  p.sigla_uf,
  p.ano,
  p.populacao,
  n.nome,
  pb.pib,
  ge.valor AS gastos_educacao,
  m.quantidade_matricula,
  i.ideb_iniciais,
  i.ideb_finais,
  a.taxa_abandono_ef_anos_iniciais,
  a.taxa_abandono_ef_anos_finais,
  -- Calculate derived metrics
  CASE WHEN p.populacao IS NOT NULL AND p.populacao > 0 
     THEN ROUND(SAFE_CAST(pb.pib AS FLOAT64) / SAFE_CAST(p.populacao AS FLOAT64), 2)
     ELSE NULL 
END AS pib_per_capita,
  CASE WHEN m.quantidade_matricula IS NOT NULL AND m.quantidade_matricula > 0 
     THEN ROUND(SAFE_CAST(ge.valor AS FLOAT64) / SAFE_CAST(m.quantidade_matricula AS FLOAT64), 2)
     ELSE NULL 
END AS gasto_por_aluno
FROM populacao p
LEFT JOIN pib pb ON p.id_municipio = pb.id_municipio AND p.ano = pb.ano
LEFT JOIN name n ON p.id_municipio = n.id_municipio
LEFT JOIN gastos_educ ge ON p.id_municipio = ge.id_municipio AND p.ano = ge.ano
LEFT JOIN matriculas m ON p.id_municipio = m.id_municipio AND p.ano = m.ano
LEFT JOIN ideb i ON p.id_municipio = i.id_municipio AND p.ano = i.ano
LEFT JOIN abandono a ON p.id_municipio = a.id_municipio AND p.ano = a.ano
"""

try:
    combined_df = bd.read_sql(silver_df)
    save_dataframe(combined_df, "silver_data", directory="data/processed/silver", file_format="csv")
except Exception as e:
    print(f"Error in combined query: {str(e)}")