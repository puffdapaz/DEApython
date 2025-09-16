import basedosdados as bd
import os
import yaml
import logging
import pandas as pd
import numpy as np
from scipy import stats
from dealib import RTS, Orientation, dea
from typing import Dict, Optional, List
from pathlib import Path
from dotenv import load_dotenv
from .save_utils import save_dataframe, save_dataframe_to_gcs

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
    logger.info("Base dos Dados configured successfully")
    
    return bucket_name

def perform_dea_analysis():
    """Run DEA on complete silver dataset, export gold results."""
    complete_cases = pd.read_csv("data\processed\silver\silver_data.csv")
    complete_cases = complete_cases[complete_cases["is_complete_grouped"] == True]

    print("Data Description:")
    print(complete_cases.groupby('ano').describe().stack())
    print(complete_cases.groupby('ano').corr(numeric_only=True))

    results, efficiency_analysis = {}, {}

    def prepare_matrices(df):
        X = df[['pib_per_capita', 'gasto_por_aluno']].to_numpy()
        y_ideb = df[['ideb_iniciais', 'ideb_finais']].to_numpy()
        abandono_iniciais = (100 - df['taxa_abandono_ef_anos_iniciais']).to_numpy().reshape(-1, 1)
        abandono_finais = (100 - df['taxa_abandono_ef_anos_finais']).to_numpy().reshape(-1, 1)
        Y = np.hstack([y_ideb, abandono_iniciais, abandono_finais])

        print(f"Input shape: {X.shape}, Output shape: {Y.shape}")
        return X, Y

    for year, subset in complete_cases.groupby("ano"):
        print(f"\nRunning DEA for year {year}")
        X, Y = prepare_matrices(subset)

        dea_models = {
            "crs_input": dea(X, Y, rts=RTS.crs, orientation=Orientation.input).eff,
            "crs_output": 1 / dea(X, Y, rts=RTS.crs, orientation=Orientation.output).eff,
            "vrs_input": dea(X, Y, rts=RTS.vrs, orientation=Orientation.input).eff,
            "vrs_output": 1 / dea(X, Y, rts=RTS.vrs, orientation=Orientation.output).eff,
            "irs_input": dea(X, Y, rts=RTS.irs, orientation=Orientation.input).eff,
            "drs_input": dea(X, Y, rts=RTS.drs, orientation=Orientation.input).eff,
        }

        eff_scores = dea_models.copy()

        print(f"Shapiro-Wilk VRS-input {year}: {stats.shapiro(eff_scores['vrs_input'])}")
        print(f"KS CRS vs VRS-input {year}: {stats.ks_2samp(eff_scores['crs_input'], eff_scores['vrs_input'])}")

        if 'crs_input' in eff_scores and 'vrs_input' in eff_scores:
            eff_scores["scale_efficiency"] = eff_scores['crs_input'] / eff_scores['vrs_input']

        returns_nature = []
        if all(k in eff_scores for k in ('crs_input', 'vrs_input', 'drs_input')):
            for i in range(len(eff_scores['crs_input'])):
                if eff_scores['crs_input'][i] == eff_scores['vrs_input'][i]:
                    returns_nature.append("Constante")
                elif eff_scores['drs_input'][i] == eff_scores['vrs_input'][i]:
                    returns_nature.append("Decrescente")
                else:
                    returns_nature.append("Crescente")
            eff_scores["returns_nature"] = returns_nature

        result_df = subset.copy()
        for name, scores in eff_scores.items():
            if name != "returns_nature" and len(scores) == len(result_df):
                result_df[f"DEA_{name}"] = scores
        if "returns_nature" in eff_scores:
            result_df["DEA_returns_nature"] = eff_scores["returns_nature"]

        results[year] = {"models": dea_models, "result_df": result_df}
        efficiency_analysis[year] = eff_scores
        
        all_results = pd.concat([year_data["result_df"] for year_data in results.values()])
        return all_results

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
        logger.info("Executing gold query...")
        
        # Validate data
        if not validate_gold_data(all_results, value_columns):
            raise ValueError("Gold data validation failed")
        
        # Save data
        local_path = Path("data/processed/gold")
        local_path.mkdir(parents=True, exist_ok=True)
        
        save_dataframe(all_results, "gold_data_complete", directory=local_path)
        save_dataframe_to_gcs(all_results, "gold_data_complete", bucket_name, layer="gold")
        
        logger.info("Gold data processing completed successfully")
        return all_results
        
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
    if 'is_complete_grouped' in df.columns:
        complete_by_year = df.groupby('ano')['is_complete_grouped'].mean()
        logger.info("Completeness by year:")
        for year, completeness in complete_by_year.items():
            logger.info(f"  {year}: {completeness:.1%} complete")
        
        total_complete = df['is_complete_grouped'].mean()
        logger.info(f"Overall completeness: {total_complete:.1%}")

if __name__ == "__main__":
    all_results, efficiency_analysis = perform_dea_analysis()
    if all_results:
        print("✅ DEA analysis completed successfully!")

        # Print comprehensive diagnostics
        print("=== GOLD DATA COMPLETENESS ANALYSIS ===")
        print(f"Total records: {len(all_results)}")
        print(f"Unique municipalities: {all_results['id_municipio'].nunique()}")
        print(f"Years covered: {sorted(all_results['ano'].unique())}")
        
        if 'is_complete_grouped' in all_results.columns:
            print("\n--- Completeness Status ---")
            complete_count = all_results['is_complete_grouped'].sum()
            incomplete_count = len(all_results) - complete_count
            print(f"Complete records: {complete_count} ({complete_count/len(all_results):.1%})")
            print(f"Incomplete records: {incomplete_count} ({incomplete_count/len(results):.1%})")
            
            # By year analysis
            print("\n--- Completeness by Year ---")
            for year in sorted(all_results['ano'].unique()):
                year_data = all_results[all_results['ano'] == year]
                year_complete = year_data['is_complete_grouped'].mean()
                print(f"{year}: {year_complete:.1%} complete")
        
        if 'missing_values_count' in all_results.columns:
            print("\n--- Missing Values Analysis ---")
            missing_stats = all_results['missing_values_count'].describe()
            print(f"Average missing values per record: {missing_stats['mean']:.2f}")
            print(f"Records with no missing values: {(all_results['missing_values_count'] == 0).sum()}")
            print(f"Records with 1-3 missing values: {((all_results['missing_values_count'] >= 1) & (gold_df['missing_values_count'] <= 3)).sum()}")
            print(f"Records with 4+ missing values: {(all_results['missing_values_count'] >= 4).sum()}")
        
        print("\n✅ Gold data processing completed successfully!")
        print("📊 Single file saved with completeness flags")
        
    else:
        print("❌ Gold layer processing failed")