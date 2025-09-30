import basedosdados as bd
import os
import yaml
import logging
import pandas as pd
from .save_utils import save as save
from typing import List, Optional
from pathlib import Path
from dotenv import load_dotenv
from .diagnostics.data_validation import validate_silver_data
from .diagnostics.model_diagnostics import analyze_silver_data

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
        raise ValueError("Missing required environment variables")
    
    bd.config.billing_project_id = billing_project_id
    
    return bucket_name

def get_silver_query() -> str:
    """Return the silver layer SQL query."""
    return """
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

def add_completeness_flags(df: pd.DataFrame, value_columns: List[str]) -> pd.DataFrame:
    """Add municipality-level completeness flag across years."""

    df = df.copy()

    # Temporary row-level flag
    tmp_flag = df[value_columns].notnull().all(axis=1)

    # Grouped completeness: all years must be complete
    df['is_complete_grouped'] = (
        df.assign(_tmp=tmp_flag)
          .groupby('id_municipio')['_tmp']
          .transform(lambda x: x.all())
    )

    return df

def process_silver_data() -> Optional[bd.Table]:
    """Process silver layer data."""
    try:
        # Load configurations
        layer, paths = load_configs()
        
        # Set up Base dos Dados
        bucket_name = setup_basedosdados()
        
        # Get and execute query
        query = get_silver_query()
        silver_df = bd.read_sql(query)
        
        value_columns = [
        'populacao', 'pib', 'gastos_educacao', 'quantidade_matricula',
        'ideb_iniciais', 'ideb_finais',
        'taxa_abandono_ef_anos_iniciais', 'taxa_abandono_ef_anos_finais',
        'pib_per_capita', 'gasto_por_aluno'
    ]

        silver_df = add_completeness_flags(silver_df, value_columns)

        # Validate data
        if not validate_silver_data(silver_df):
            raise ValueError("Silver data validation failed")
        
        # Data diagnostics
        analyze_silver_data(silver_df)

        # Save data
        local_path = Path(paths["paths"]["silver"])
        layer = paths["layers"]["silver"]
        local_path.mkdir(parents=True, exist_ok=True)
        
        save.save_dataframe(silver_df, "silver_data", directory=local_path)
        save.save_dataframe_to_gcs(silver_df, "silver_data", bucket_name, layer=layer)
        
        logger.info(f"Processing completed. Data saved at {local_path} and GCP://{bucket_name}/{layer} successfully")
        return silver_df
        
    except Exception as e:
        logger.error(f"Silver data processing failed: {e}")
        return None

if __name__ == "__main__":
    silver_df = process_silver_data()