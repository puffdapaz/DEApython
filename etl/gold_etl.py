import basedosdados as bd
import os
import yaml
import logging
import pandas as pd
from typing import Dict, Optional, List
from pathlib import Path
from dotenv import load_dotenv
from save import save_dataframe, save_dataframe_to_gcs

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
    """Set up Base dos Dados configuration."""
    load_dotenv()
    billing_project_id = os.getenv("billing_project_id")
    bucket_name = os.getenv("gcp_bucket_name")
    
    if not billing_project_id or not bucket_name:
        raise ValueError("Missing required environment variables")
    
    bd.config.billing_project_id = billing_project_id
    logger.info("Base dos Dados configured successfully")
    
    return bucket_name

def get_gold_query() -> str:
    """Return the gold layer SQL query."""
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

def validate_gold_data(df: pd.DataFrame, value_columns: List[str]) -> bool:
    """Validate gold data quality."""
    if df is None or df.empty:
        logger.error("Gold data is empty")
        return False
    
    required_columns = ['id_municipio', 'sigla_uf', 'ano'] + value_columns
    missing_columns = set(required_columns) - set(df.columns)
    
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    
    logger.info(f"Gold data validation passed. Shape: {df.shape}")
    return True

def add_completeness_flags(df: pd.DataFrame, value_columns: List[str]) -> pd.DataFrame:
    """Add completeness flags to the dataframe."""
    df = df.copy()
    
    # Add simple completeness flag for each year
    df['is_complete'] = df[value_columns].notnull().all(axis=1)
    
    # Add missing values count for analysis
    df['missing_values_count'] = df[value_columns].isnull().sum(axis=1)
    
    return df

def process_gold_data() -> Optional[pd.DataFrame]:
    """Process gold layer data with completeness flags."""
    try:
        logger.info("Starting gold data processing")
        
        # Load configurations
        dea_config, paths = load_configs()
        
        # Get value columns from config
        value_columns = dea_config.get('gold', {}).get('value_columns', [
            'populacao', 'pib', 'gastos_educacao', 'quantidade_matricula',
            'ideb_iniciais', 'ideb_finais', 
            'taxa_abandono_ef_anos_iniciais', 'taxa_abandono_ef_anos_finais',
            'pib_per_capita', 'gasto_por_aluno'
        ])
        
        # Set up Base dos Dados
        bucket_name = setup_basedosdados()
        
        # Get and execute query
        query = get_gold_query()
        logger.info("Executing gold query...")
        gold_df = bd.read_sql(query)
        
        # Add completeness flags
        gold_df = add_completeness_flags(gold_df, value_columns)
        
        # Validate data
        if not validate_gold_data(gold_df, value_columns):
            raise ValueError("Gold data validation failed")
        
        # Save data
        local_path = Path("data/processed/gold")
        local_path.mkdir(parents=True, exist_ok=True)
        
        save_dataframe(gold_df, "gold_data_complete", directory=local_path)
        save_dataframe_to_gcs(gold_df, "gold_data_complete", bucket_name, layer="gold")
        
        logger.info("Gold data processing completed successfully")
        return gold_df
        
    except Exception as e:
        logger.error(f"Gold data processing failed: {e}")
        return None

def analyze_gold_data(df: pd.DataFrame):
    """Generate analysis of gold data."""
    if df is None:
        return
    
    logger.info("Gold Data Analysis:")
    logger.info(f"Total records: {len(df)}")
    logger.info(f"Years: {sorted(df['ano'].unique())}")
    logger.info(f"States: {df['sigla_uf'].nunique()}")
    logger.info(f"Municipalities: {df['id_municipio'].nunique()}")
    
    # Completeness analysis
    if 'is_complete' in df.columns:
        complete_by_year = df.groupby('ano')['is_complete'].mean()
        logger.info("Completeness by year:")
        for year, completeness in complete_by_year.items():
            logger.info(f"  {year}: {completeness:.1%} complete")
        
        total_complete = df['is_complete'].mean()
        logger.info(f"Overall completeness: {total_complete:.1%}")

if __name__ == "__main__":
    gold_df = process_gold_data()
    if gold_df is not None:
        analyze_gold_data(gold_df)
        
        # Print comprehensive diagnostics
        print("=== GOLD DATA COMPLETENESS ANALYSIS ===")
        print(f"Total records: {len(gold_df)}")
        print(f"Unique municipalities: {gold_df['id_municipio'].nunique()}")
        print(f"Years covered: {sorted(gold_df['ano'].unique())}")
        
        if 'is_complete' in gold_df.columns:
            print("\n--- Completeness Status ---")
            complete_count = gold_df['is_complete'].sum()
            incomplete_count = len(gold_df) - complete_count
            print(f"Complete records: {complete_count} ({complete_count/len(gold_df):.1%})")
            print(f"Incomplete records: {incomplete_count} ({incomplete_count/len(gold_df):.1%})")
            
            # By year analysis
            print("\n--- Completeness by Year ---")
            for year in sorted(gold_df['ano'].unique()):
                year_data = gold_df[gold_df['ano'] == year]
                year_complete = year_data['is_complete'].mean()
                print(f"{year}: {year_complete:.1%} complete")
        
        if 'missing_values_count' in gold_df.columns:
            print("\n--- Missing Values Analysis ---")
            missing_stats = gold_df['missing_values_count'].describe()
            print(f"Average missing values per record: {missing_stats['mean']:.2f}")
            print(f"Records with no missing values: {(gold_df['missing_values_count'] == 0).sum()}")
            print(f"Records with 1-3 missing values: {((gold_df['missing_values_count'] >= 1) & (gold_df['missing_values_count'] <= 3)).sum()}")
            print(f"Records with 4+ missing values: {(gold_df['missing_values_count'] >= 4).sum()}")
        
        print("\n✅ Gold data processing completed successfully!")
        print("📊 Single file saved with completeness flags")
        
    else:
        print("❌ Gold layer processing failed")