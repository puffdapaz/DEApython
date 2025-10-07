"""
Gold Layer Data Modeling Workflow
This module handles and saves DEA modeling and data diagnostics
to both local storage and Google Cloud Storage.
"""
import os
import yaml
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Tuple, Dict, List
from dotenv import load_dotenv
from dealib import RTS, Orientation, dea
from .diagnostics.data_validation import gold_schema, validate_data
from .diagnostics import model_diagnostics as md
from .save_utils import save as save

# Set Up Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def load_configs() -> dict:
    """Load configuration from path.yml"""
    try:
        with open("configs/path.yml", "r") as f:
            paths = yaml.safe_load(f)
        return paths
    except Exception as e:
        logger.error(f"Error loading configs: {e}")
        raise

def setup_gcp_bd() -> str:
    """GCP credentials configuration to access Base dos Dados."""
    load_dotenv()
    billing_project_id = os.getenv("billing_project_id")
    bucket_name = os.getenv("gcp_bucket_name")
    
    if not billing_project_id or not bucket_name:
        raise ValueError("Missing required environment variables")
    return bucket_name

def load_gold_data() -> pd.DataFrame:
    """
    Load processed silver data for DEA analysis
    Returns:
        pd.DataFrame: Silver layer data with completeness flags
    Raises:
        ValueError: If data is missing.
    """
    try:
        df_full = pd.read_parquet("data/processed/silver/silver_data.parquet")
        if df_full.empty:
            raise ValueError("Silver data file is empty")
        return df_full
    except Exception as e:
        logger.error("Error loading data: %s", e)
        raise

def use_completeness_flags(df_full: pd.DataFrame) -> pd.DataFrame:
    """
    Filter dataset to use only complete cases for DEA analysis.
    Args:
        df_full: Silver DataFrame with completeness flags
    Returns:
        DataFrame containing only complete cases
    Raises:
        ValueError: If no complete cases are found
    """
    df_complete = df_full[df_full["is_complete_grouped"] == True]
    if df_complete.empty:
        raise ValueError("No complete cases found in dataset")
    return df_complete

def prepare_matrices(subset: pd.DataFrame)-> Tuple[np.ndarray, np.ndarray]:
    """
    Prepare input and output matrices for DEA analysis.
    Args:
        df: DataFrame containing the data for DEA
    Returns:
        Tuple of (X, Y) where X is input matrix and Y is output matrix
    Raises:
        ValueError: If required columns are missing from the data
    """
    try:
        X = subset[["pib_per_capita",
                    "gasto_por_aluno"]].to_numpy()
        y_ideb = subset[["ideb_iniciais",
                            "ideb_finais"]].to_numpy()
        abandono_iniciais = (100 - subset["taxa_abandono_ef_anos_iniciais"]).to_numpy().reshape(-1, 1)
        abandono_finais = (100 - subset["taxa_abandono_ef_anos_finais"]).to_numpy().reshape(-1, 1)
        Y = np.hstack([y_ideb,
                        abandono_iniciais,
                        abandono_finais])
        return X, Y
    except Exception as e:
            logger.error("Error preparing DEA matrices: %s", e)
            raise
    
def run_dea_models(X: np.ndarray, Y: np.ndarray) -> Dict[str, np.ndarray]:
    """
    Run DEA models with different returns-to-scale assumptions.
    Return dict of numpy arrays (same order as rows in X/Y)
    """
    dea_results = {}
    try:
        dea_results["crs_input"] = dea(X, Y, rts=RTS.crs, orientation=Orientation.input).eff
        dea_results["crs_output"] = 1.0 / dea(X, Y, rts=RTS.crs, orientation=Orientation.output).eff
        dea_results["vrs_input"] = dea(X, Y, rts=RTS.vrs, orientation=Orientation.input).eff
        dea_results["vrs_output"] = 1.0 / dea(X, Y, rts=RTS.vrs, orientation=Orientation.output).eff
        dea_results["irs_input"] = dea(X, Y, rts=RTS.irs, orientation=Orientation.input).eff
        dea_results["drs_input"] = dea(X, Y, rts=RTS.drs, orientation=Orientation.input).eff
        return dea_results
    except Exception as e:
        logger.error("Error running DEA models: %s", e)
        raise

def additional_derived_metrics(subset: pd.DataFrame, dea_results: Dict[str, np.ndarray]) -> pd.DataFrame:
    """
    Calculate derived year subset DEA metrics from base efficiency scores
    return a DataFrame equal to subset with DEA_* columns added.
    """
    try:
        result_df = subset.copy()
        eff = dict(dea_results)
        eff["scale_efficiency"] = eff["crs_input"] / eff["vrs_input"]

        returns_nature = np.where(
            eff["crs_input"] == eff["vrs_input"], "Constante",
            np.where(eff["drs_input"] == eff["vrs_input"], "Decrescente", "Crescente"))

        # attach arrays as new columns, prefixing with DEA_
        for k, arr in eff.items():
            # arr must be same length as result_df
            result_df[f"DEA_{k}"] = arr

        result_df["DEA_returns_nature"] = returns_nature

        return result_df
    except Exception as e:
        logger.error("Error calculating derived metrics: %s", e)
        raise

def results_wrapper(df_full: pd.DataFrame,
                    dea_results: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Merge DEA results back into the full dataframe
    """
    try:
        # dea_df = pd.concat(dea_results, ignore_index=True)
        # gold_df = df_full.merge(
        #     dea_df[["id_municipio", "ano"] + [c for c in dea_df.columns if c.startswith("DEA_")]],
        #     on=["id_municipio", "ano"],
        #     how="left")
        # gold_df["id_municipio"] = gold_df["id_municipio"].astype("str")
        # return gold_df
        dea_columns = [c for c in dea_results.columns if c.startswith("DEA_")]
        merge_cols = ["id_municipio", "ano"] + dea_columns

        gold_df = df_full.merge(
            dea_results[merge_cols],
            on=["id_municipio", "ano"],
            how="left")
        gold_df["id_municipio"] = gold_df["id_municipio"].astype(str)
        return gold_df
    except Exception as e:
        logger.error("Error merging DEA results: %s", e)
        raise

def run_gold_model(df_full: pd.DataFrame) -> List[pd.DataFrame]:
    """
    Run DEA analysis grouped by year.
    Args:
        complete_cases: DataFrame containing only complete cases
    Returns:
        List of DataFrames with DEA results for each year
    """
    df_complete = use_completeness_flags(df_full)
    results = []
    
    for year, year_data in df_complete.groupby("ano"):
        print(f"Running DEA for {year}")  

        try:
            X, Y = prepare_matrices(year_data)
            dea_results = run_dea_models(X, Y)
            year_results = additional_derived_metrics(year_data, dea_results)
            results.append(year_results)
        except Exception as e:
                logger.error("DEA failed for year %d: %s", year, e)
                raise
    results_df = pd.concat(results, ignore_index=True)
    gold_df = results_wrapper(df_full, results_df)
    return gold_df

def validate_gold(gold_df: pd.DataFrame) -> None:
    """
    Validate gold layer DataFrame against predefined schema.
    Args:
        dataframe (pd.DataFrame): 
            DataFrame keyed by dataset name.
    Raises:
        ValueError: If validation fails for any DataFrame.
    """
    if not validate_data(gold_df,
                         schema=gold_schema,
                         name="Gold"):
        raise ValueError("Gold data validation failed")  

def run_diagnostics(gold_df: pd.DataFrame) -> None:
    """
    Run comprehensive diagnostics on gold data
    """
    try:
        md.analyze_data(gold_df,
                        name="Gold")
        md.run_diagnostics(gold_df,
                           log=True)
    except Exception as e:
        logger.error("Error during gold data diagnostics: %s", e)
        raise

def save_gold(gold_df: pd.DataFrame,
              paths: dict,
              bucket_name: str) -> None:
    """
    Save gold DataFrame and diagnostics locally and to GCS.
    Args:
        gold_df (pd.DataFrame): Silver DataFrame.
        paths (dict): Dictionary with local paths and layers.
        bucket_name (str): GCP bucket name.
    """
    local_path = Path(paths["paths"]["gold"])
    layer = paths["layers"]["gold"]
    local_path.mkdir(parents=True,
                    exist_ok=True)
    save.save_data(gold_df,
                "gold_data",
                directory=local_path)
    save.save_data_to_gcs(gold_df,
                        "gold_data",
                        bucket_name,
                        layer=layer)
    logger.info(f"Data saved at {local_path} and GCP://{bucket_name}/{layer} successfully")

def model_gold_data() -> Optional[pd.DataFrame]:
        try:
            paths = load_configs()
            bucket_name = setup_gcp_bd()

            df_full = load_gold_data()
            gold_df = run_gold_model(df_full)

            validate_gold(gold_df)
            run_diagnostics(gold_df)

            save_gold(gold_df, paths, bucket_name)
            print(f"Modeling completed")
            return gold_df
        except Exception as e:
                logger.error(f"Modeling failed: {e}")
                return None

if __name__ == "__main__":
    gold_df = model_gold_data()
