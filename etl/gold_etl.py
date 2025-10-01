import os
import yaml
import logging
import pandas as pd
import numpy as np
from .save_utils import save as save
from dealib import RTS, Orientation, dea
from pathlib import Path
from dotenv import load_dotenv
from .diagnostics.data_validation import gold_schema, validate_data
from .diagnostics import model_diagnostics as md
from typing import Optional

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
    
    return bucket_name

def perform_dea_analysis() -> pd.DataFrame:
    """Run DEA on complete Silver dataset and return one Gold DataFrame."""

    # 1. Load Silver data
    df_full = pd.read_parquet("data/processed/silver/silver_data.parquet")
    
    # Only use complete cases for DEA calculations
    df_complete = df_full[df_full["is_complete_grouped"] == True]
    if df_complete.empty:
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

    for year, subset in df_complete.groupby("ano"):
        print(f"Running DEA for {year}")
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

        returns_nature = np.where(
            eff_scores["crs_input"] == eff_scores["vrs_input"], "Constante",
            np.where(eff_scores["drs_input"] == eff_scores["vrs_input"], "Decrescente", "Crescente")
        )
        eff_scores["returns_nature"] = returns_nature.tolist()

        result_df = subset.copy()
        for k, v in eff_scores.items():
            if k != "returns_nature":
                result_df[f"DEA_{k}"] = v
        result_df["DEA_returns_nature"] = eff_scores["returns_nature"]

        results.append(result_df)

    # 4. Merge into one Gold file
    dea_df = pd.concat(results, ignore_index=True)
    gold_df = df_full.merge(
        dea_df[["id_municipio", "ano"] + [c for c in dea_df.columns if c.startswith("DEA_")]],
        on=["id_municipio", "ano"],
        how="left"
    )

    # Explicitly cast columns to expected types
    gold_df["id_municipio"] = gold_df["id_municipio"].astype("str")

    return gold_df

def process_gold_data() -> Optional[pd.DataFrame]:
    try:
        dea_config, paths = load_configs()
    
        # Set up Base dos Dados
        bucket_name = setup_basedosdados()

        gold_df = perform_dea_analysis()

        # Validate data
        if not validate_data(gold_df, schema=gold_schema, name="Gold"):
            raise ValueError("Gold data validation failed")

        # Data diagnostics
        md.analyze_data(gold_df, name="Gold")
        md.run_diagnostics(gold_df, log=True)

        # Save single Gold file
        local_path = Path(paths["paths"]["gold"])
        layer = paths["layers"]["gold"]
        local_path.mkdir(parents=True, exist_ok=True)
        
        save.save_data(gold_df, "gold_data", directory=local_path)
        save.save_data_to_gcs(gold_df, "gold_data", bucket_name, layer=layer)

        print(f"Modeling completed")
        logger.info(f"Data saved at {local_path} and GCP://{bucket_name}/{layer} successfully")
        return gold_df

    except Exception as e:
            logger.error(f"Modeling failed: {e}")
            return None

if __name__ == "__main__":
    gold_df = process_gold_data()
