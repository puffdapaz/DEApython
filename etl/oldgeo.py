"""
Geodata fetching, cleaning, and exporting Brazilian geospatial data
as TopoJSON for analytical consumption
"""

import os
import logging
from pathlib import Path
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
def load_configs(config_path: str = "configs/path.yml") -> dict:
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

def fetch_geodata(year: int) -> gpd.GeoDataFrame:
    """
    Fetch geodata for Brazilian states and municipalities from geobr.
    Args:
        year (int): Reference year for boundaries (default: 2019).
    Returns:
        muni_gdf: GeoDataFrame (municipalities)
        state_gdf: GeoDataFrame (states)
    Raises:
        Exception: If fetching geodata fails.
    """
    try:
        muni_gdf = geobr.read_municipality(code_muni = "all",
                                           year = year)
        muni_gdf = gpd.GeoDataFrame(muni_gdf).rename(columns={"code_muni": "city_id",
                                                              "name_muni": "city_name",
                                                              "code_state": "state_id",
                                                              "name_state": "state_name"
                                                             })
        state_gdf = geobr.read_state(year=year)
        state_gdf = gpd.GeoDataFrame(state_gdf).rename(columns={"code_state": "state_id",
                                                                "name_state": "state_name"                                                                
                                                               })
        return muni_gdf, state_gdf
    except Exception as e:
        logger.error(f"Error fetching geobr data: {e}")
        raise

# def merge_geodata(data : pd.DataFrame,
#                   geodata : gpd.GeoDataFrame) -> gpd.GeoDataFrame:
#     """
#     Merge the analytical dataset with municipality geodata.
#     Args:
#         df (pd.DataFrame): Silver-layer analytical dataset with `city_id` column.
#         geodata (gpd.GeoDataFrame): Municipality polygons from geobr.
#     Returns:
#         gpd.GeoDataFrame: Enriched dataset with geometries.
#     Raises:
#         Exception: If merging geodata fails.
#     """
#     try:
#         df = data.copy()
#         df["city_id_int"] = df["city_id"].astype(str).astype(int)
#         geodata["code_muni"] = geodata["code_muni"].astype(int)

#         merged_map = df.merge(geodata[["code_muni", "geometry"]],
#                               how="left",
#                               left_on="city_id_int",
#                               right_on="code_muni")
#         merged_map = merged_map.drop(columns=["city_id_int", "code_muni"])

#         merged_map = gpd.GeoDataFrame(merged_map,
#                                       geometry = "geometry",
#                                       crs = geodata.crs)
#         merged_map["geometry"] = merged_map.geometry.simplify(tolerance = 0.01)
#         return merged_map
#     except Exception as e:
#         logger.error(f"Error merging geobr data: {e}")
#         raise

# ---------------------------------------------------------------------
# Cleaning helpers
# ---------------------------------------------------------------------
def clean_strings(df: pd.DataFrame, cols: list):
    """Remove NaNs and enforce string dtype."""
    df = df.replace({pd.NA: None, "nan": None, "NaN": None})
    for col in cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(".0", "", regex=False)
                .str.strip()
            )
    return df

def pad_state_id(df):
    """Ensure state_id is always 2 digits."""
    df["state_id"] = df["state_id"].astype(str).str.zfill(2)
    return df

def simplify_geometries(df, tolerance=0.03):
    """Simplify geometries to reduce file size."""
    df["geometry"] = df.geometry.simplify(tolerance)
    return df

# ---------------------------------------------------------------------
# Geodata JSON Conversion and Export
# ---------------------------------------------------------------------
def convert_to_topojson(muni_gdf: gpd.GeoDataFrame,
                        state_gdf: gpd.GeoDataFrame,) -> str:
    """
    Convert GeoDataFrame to TopoJSON for Power BI.
    Args:
        muni_gdf: GeoDataFrame with municipality geometries
        state_gdf: GeoDataFrame with state geometries
    Returns:
        topo_json: str: TopoJSON file as string
    Raises:
        Exception: If converting geodata fails.
    """
    try:
        muni_gdf = (muni_gdf.to_crs(epsg=4326)
                            .copy())
        state_gdf = (state_gdf.to_crs(epsg=4326)
                              .copy())
        
        # Clean & type normalize
        muni_gdf = clean_strings(muni_gdf, ["city_id",
                                            "state_id",
                                            "city_name", 
                                            "state_name"])
        state_gdf = clean_strings(state_gdf, ["state_id", 
                                              "state_name"])

        muni_gdf = pad_state_id(muni_gdf)
        state_gdf = pad_state_id(state_gdf)

        # Define hierarchy
        state_gdf["id"] = state_gdf["state_id"]
        muni_gdf["id"] = muni_gdf["city_id"]

        state_gdf["parent"] = ""
        muni_gdf["parent"] = muni_gdf["state_id"]

        # Simplify shapes
        muni_gdf = simplify_geometries(muni_gdf)
        state_gdf = simplify_geometries(state_gdf)

        # Combine into one unified table
        keep = ["id", 
                "city_id", 
                "state_id", 
                "city_name", 
                "state_name", 
                "parent", 
                "geometry"]
        combined = pd.concat([state_gdf, muni_gdf], ignore_index=True)[keep]
        combined = gpd.GeoDataFrame(combined, geometry="geometry", crs=4326)

        # Export to TopoJSON
        topo = tp.Topology(combined, prequantize=False)
        topo_json = topo.to_json().replace("NaN", '""')
        return topo_json
    except Exception as e:
        logger.error(f"Error converting to TopoJSON: {e}")
        raise

def save_json(topo_json: str) -> None:
    """
    Save TopoJSON file locally and to GCS.
    Args:
        topo_json str: TopoJSON file generated by convert_to_topojson()
    Raises:
        Exception: For other unexpected errors.
    """
    try:
        paths = load_configs()
        bucket_name = setup_gcp_bd()
        local_path = Path(paths["paths"]["gold"])
        layer = paths["layers"]["gold"]
        local_path.mkdir(parents=True,
                         exist_ok=True)
        save_data(topo_json,
                  "geo_json",
                  directory=local_path,
                  file_format="json"
                  )
        save_data_to_gcs(topo_json,
                         "geo_json",
                         bucket_name,
                         layer=layer,
                         file_format="json"
                         )
        print(f"geodata saved as JSON")
    except Exception as e:
        logger.error(f"Error saving JSON: {e}")
        raise