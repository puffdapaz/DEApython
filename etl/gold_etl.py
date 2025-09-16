import os
import yaml
import logging
import pandas as pd
import numpy as np
from scipy import stats
from dealib import RTS, Orientation, dea
from pathlib import Path
from dotenv import load_dotenv
from .save_utils import save_dataframe, save_dataframe_to_gcs

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
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
    
    logger.info("Base dos Dados configured successfully")
    
    return bucket_name

def perform_dea_analysis() -> pd.DataFrame:
    """Run DEA on complete Silver dataset and return one Gold DataFrame."""

    # 1. Load Silver data
    df = pd.read_csv("data/processed/silver/silver_data.csv")

    # 2. Filter complete cases
    df = df[df["is_complete_grouped"] == True]
    if df.empty:
        raise ValueError("No complete cases found in Silver dataset")

    results = []

    # 3. DEA per year
    def prepare_matrices(subset):
        X = subset[["pib_per_capita", "gasto_por_aluno"]].to_numpy()
        y_ideb = subset[["ideb_iniciais", "ideb_finais"]].to_numpy()
        abandono_iniciais = (100 - subset["taxa_abandono_ef_anos_iniciais"]).to_numpy().reshape(-1, 1)
        abandono_finais = (100 - subset["taxa_abandono_ef_anos_finais"]).to_numpy().reshape(-1, 1)
        Y = np.hstack([y_ideb, abandono_iniciais, abandono_finais])
        return X, Y

    for year, subset in df.groupby("ano"):
        logger.info(f"Running DEA for year {year}")
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
        eff_scores["scale_efficiency"] = eff_scores["crs_input"] / eff_scores["vrs_input"]

        returns_nature = []
        for i in range(len(eff_scores["crs_input"])):
            if eff_scores["crs_input"][i] == eff_scores["vrs_input"][i]:
                returns_nature.append("Constante")
            elif eff_scores["drs_input"][i] == eff_scores["vrs_input"][i]:
                returns_nature.append("Decrescente")
            else:
                returns_nature.append("Crescente")
        eff_scores["returns_nature"] = returns_nature

        result_df = subset.copy()
        for k, v in eff_scores.items():
            if k != "returns_nature":
                result_df[f"DEA_{k}"] = v
        result_df["DEA_returns_nature"] = eff_scores["returns_nature"]

        results.append(result_df)

    # 4. Merge into one Gold file
    gold_df = pd.concat(results, ignore_index=True)
    return gold_df

def validate_gold_data(gold_df) -> bool:
    """Validate silver data quality."""
    if gold_df is None or gold_df.empty:
        logger.error("Silver data is empty or None")
        return False
    
    required_columns = [
        'DEA_crs_input', 'DEA_crs_output', 'DEA_vrs_input', 'DEA_vrs_output',
        'DEA_irs_input', 'DEA_drs_input', 'DEA_scale_efficiency', 'DEA_returns_nature'
    ]
    
    missing_columns = set(required_columns) - set(gold_df.columns)
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    
    # Check for reasonable data ranges
    validation_checks = {
        'DEA_crs_input': (lambda x: 0 <= x <= 1, "DEA interval should be 0-1"),
        'DEA_crs_output': (lambda x: 0 <= x <= 1, "DEA interval should be 0-1"),
        'DEA_vrs_input': (lambda x: 0 <= x <= 1, "DEA interval should be 0-1"),
        'DEA_vrs_output': (lambda x: 0 <= x <= 1, "DEA interval should be 0-1"),
        'DEA_irs_input': (lambda x: 0 <= x <= 1, "DEA interval should be 0-1"),
        'DEA_drs_input': (lambda x: 0 <= x <= 1, "DEA interval should be 0-1"),
        'DEA_scale_efficiency': (lambda x: 0 <= x <= 1, "DEA interval should be 0-1")
    }
    
    for col, (check_func, message) in validation_checks.items():
        if col in gold_df.columns:
            invalid_count = gold_df[gold_df[col].notna() & ~gold_df[col].apply(check_func)].shape[0]
            if invalid_count > 0:
                logger.warning(f"{invalid_count} records with invalid {col}: {message}")
    
    logger.info(f"Silver data validation passed. Shape: {gold_df.shape}")
    return True

def process_gold_data() -> pd.DataFrame:
    logger.info("Starting Gold data modeling process...")
    dea_config, paths = load_configs()
  
    # Set up Base dos Dados
    bucket_name = setup_basedosdados()

    gold_df = perform_dea_analysis()

    # Validate data
    if not validate_gold_data(gold_df):
        raise ValueError("Gold data validation failed")

    # Save single Gold file
    local_path = Path("data/processed/gold")
    local_path.mkdir(parents=True, exist_ok=True)
    
    save_dataframe(gold_df, "gold_data", directory=local_path)
    save_dataframe_to_gcs(gold_df, "gold_data", bucket_name, layer="gold")

    logger.info("✅ Gold data processing completed")
    return gold_df

def analyze_gold_data(gold_df):
    """Generate analysis of gold data."""
    if gold_df is None:
        return
    
    logger.info("Silver Data Analysis:")
    logger.info(f"Total records: {len(gold_df)}")
    logger.info(f"Years: {gold_df['ano'].unique()}")
    logger.info(f"Municipalities: {gold_df['id_municipio'].nunique()}")

if __name__ == "__main__":
    gold_df = process_gold_data()
    if gold_df is not None:
        analyze_gold_data(gold_df)
        logger.info("Silver layer processing completed successfully")
    else:
        logger.error("Silver layer processing failed")
