"""
Geodata fetching, processing, simplification, and export
of Brazilian boundary data as optimized TopoJSON.
"""

import os
import logging
from pathlib import Path
from typing import Dict, Tuple, List
import pandas as pd
import geopandas as gpd
import geobr
import topojson as tp
import yaml
from dotenv import load_dotenv
from .save_utils import save_data, save_data_to_gcs

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Config Loading
# ---------------------------------------------------------------------
def load_configs(config_path: str = "configs/path.yaml") -> Dict:
    """
    Load YAML configuration for paths and layers.
    Args:
        config_path: Path to the YAML configuration file
    Returns:
        Dict containing configuration parameters
    Raises:
        Exception: For other unexpected errors
    """
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error loading configs: {e}")
        raise

def setup_gcp_bd() -> str:
    """
    Configure GCP credentials and retrieve bucket name from environment variables.
    Returns:
        str: GCP bucket name for data storage
    Raises:
        ValueError: If required environment variables are missing
    """
    load_dotenv()
    billing_project_id = os.getenv("billing_project_id")
    bucket_name = os.getenv("gcp_bucket_name")
    if not billing_project_id or not bucket_name:
        raise ValueError("Missing required environment variables")
    return bucket_name

# ---------------------------------------------------------------------
# Geodata Extraction and Merging
# ---------------------------------------------------------------------

def fetch_geodata(year: int = 2017) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Load municipal and state geometries from geobr.
    Args:
        year (int): Reference year for geobr shapes.
    Returns:
        Tuple[GeoDataFrame, GeoDataFrame]:
            Municipalities and states GeoDataFrames.
    Raises:
        Exception: For unexpected errors fetching geobr data.
    """
    try:
        muni_gdf = geobr.read_municipality(code_muni = "all",
                                           year = year)
        muni_gdf = gpd.GeoDataFrame(muni_gdf).rename(columns = {"code_muni": "city_id",
                                                                "name_muni": "city_name",
                                                                "code_state": "state_id",
                                                                "name_state": "state_name"
                                                             })
        state_gdf = geobr.read_state(year = year)
        state_gdf = gpd.GeoDataFrame(state_gdf).rename(columns = {"code_state": "state_id",
                                                                  "name_state": "state_name"                                                                
                                                               })
        return muni_gdf, state_gdf
    except Exception as e:
        logger.error(f"Error fetching geobr data: {e}")
        raise

# ---------------------------------------------------------------------
# Cleaning helper
# ---------------------------------------------------------------------
def clean_geodata(df: gpd.GeoDataFrame, cols: List[str]) -> gpd.GeoDataFrame:
    """
    Normalize identifier columns and remove NaN-like values.
    Args:
        df (GeoDataFrame): Input dataset.
    Returns:
        GeoDataFrame: Cleaned dataset.
    Raises:
        Exception: For unexpected errors cleaning data.
    """
    try:
        df = df.copy()
        df = df.replace({pd.NA: None, 
                         "nan": None, 
                         "NaN": None})
        for col in cols:
            if col in df.columns:
                df[col] = (df[col]
                           .astype(str)
                           .str.replace(".0", "", regex = False)
                           .str.strip()
                          )
        if "state_id" in df.columns:
            df["state_id"] = df["state_id"].str.zfill(2)
        return df
    except Exception as e:
        logger.error(f"Error cleaning geodata: {e}")
        raise

def prepare_geodata(muni_gdf: gpd.GeoDataFrame,
                    state_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Final preparation for TopoJSON:
        - project to WGS84
        - clean identifiers
        - assign hierarchical IDs
        - combine states + municipalities
    Args:
        muni_gdf (GeoDataFrame): Municipality shapes.
        state_gdf (GeoDataFrame): State shapes.
    Returns:
        GeoDataFrame: Combined hierarchical dataset.
    Raises:
        Exception: For unexpected errors preparing geobr data.
    """    
    try:
        # Convert to standard CRS first
        muni_gdf = (muni_gdf.to_crs(epsg = 4326)
                            .copy())
        state_gdf = (state_gdf.to_crs(epsg = 4326)
                              .copy())
        # Clean string columns
        muni_gdf = clean_geodata(muni_gdf, ["city_id",
                                            "state_id",
                                            "city_name", 
                                            "state_name"])
        state_gdf = clean_geodata(state_gdf, ["state_id", 
                                              "state_name"])
        # Define hierarchy
        state_gdf["id"] = state_gdf["state_id"]
        muni_gdf["id"] = muni_gdf["city_id"]
        state_gdf["parent"] = ""
        muni_gdf["parent"] = muni_gdf["state_id"]
        state_gdf["city_id"] = ""
        state_gdf["city_name"] = ""
        # Combine into one unified table
        keep = ["id", 
                "city_id", 
                "state_id", 
                "city_name", 
                "state_name", 
                "parent", 
                "geometry"]
        combined_gdf = pd.concat([state_gdf, 
                                  muni_gdf], 
                                  ignore_index = True
                                )[keep]
        combined_gdf = gpd.GeoDataFrame(combined_gdf, 
                                        geometry = "geometry", 
                                        crs = 4326)
        return combined_gdf
    except Exception as e:
        logger.error(f"Error preparing geodata: {e}")
        raise

# ---------------------------------------------------------------------
# Geodata JSON Conversion and Export
# ---------------------------------------------------------------------
def convert_geodata(muni_gdf: gpd.GeoDataFrame, 
                    state_gdf: gpd.GeoDataFrame) -> str:
    """
    Convert combined geodata into TopoJSON.
    Args:
        muni_gdf (GeoDataFrame): Municipalities.
        state_gdf (GeoDataFrame): States.
    Returns:
        str: TopoJSON text.
    Raises:
        Exception: If conversion fails.
    """
    try:
        combined_gdf = prepare_geodata(muni_gdf, 
                                       state_gdf)
        combined_gdf["geometry"] = combined_gdf.geometry.simplify(tolerance = 0.03)

        # Export to TopoJSON
        topo = tp.Topology(combined_gdf, 
                           prequantize = 1e5)
        topo_json = (topo.to_json()
                         .replace("NaN", '""'))
        return topo_json
    except Exception as e:
        logger.error(f"Error converting to TopoJSON: {e}")
        raise

def save_json(topo_json: str) -> None:
    """
    Save TopoJSON file locally and to GCS.
    Args:
        topo_json (str): JSON topology to save.
    Raises:
        Exception: If file cannot be saved or uploaded.
    """
    try:
        paths = load_configs()
        bucket_name = setup_gcp_bd()
        local_path = Path(paths["paths"]["gold"])
        layer = paths["layers"]["gold"]
        local_path.mkdir(parents = True,
                         exist_ok = True)
        save_data(topo_json,
                  "geo_json",
                  directory = local_path,
                  file_format = "json"
                  )
        save_data_to_gcs(topo_json,
                         "geo_json",
                         bucket_name,
                         layer = layer,
                         file_format = "json"
                         )
        print(f"geodata saved as JSON")
    except Exception as e:
        logger.error(f"Error saving JSON: {e}")
        raise

def geographical_features(year: int = 2017) -> str:
    """
    Geographical workflow for B.I. consumption:
    load, prepare, convert, save.
    Args:
        year (int): Reference year.
    Returns:
        str: Produced TopoJSON.
    Raises:
        Exception: For any pipeline-level failure.
    """
    try:
        muni_gdf, state_gdf = fetch_geodata(year)
        topo_json = convert_geodata(muni_gdf, 
                                    state_gdf)
        save_json(topo_json)
        return topo_json
    except Exception as e:
        logger.error(f"Error in geographical_features: {e}")
        raise