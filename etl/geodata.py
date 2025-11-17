"""
Geodata extraction and merging utilities for Brazilian municipality data.

This module integrates Brazilian municipality geospatial data from the 
geobr database into analytical datasets. Provides functions for fetching
municipality boundaries and merging them with analytical data.
"""

import os
from pathlib import Path
import logging
import geopandas as gpd
import geobr
import pandas as pd
import topojson as tp
import json
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
    Fetch geodata for Brazilian municipalities from geobr.
    Args:
        year (int): Reference year for the municipality boundaries (default: 2019).
    Returns:
        gpd.GeoDataFrame: Municipality geodata.
    Raises:
        Exception: If fetching geodata fails.
    """
    try:
        gdf = geobr.read_municipality(code_muni = "all",
                                      year = year)
        gdf = gpd.GeoDataFrame(gdf).rename(columns={"code_muni": "city_id",
                                                    "code_state": "state_id",
                                                    "abbrev_state": "state_abbr"
})
        return gdf
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
# Geodata JSON Conversion and Export
# ---------------------------------------------------------------------

def convert_to_topojson(gdf: gpd.GeoDataFrame) -> str:
    """
    Convert GeoDataFrame to TopoJSON for Power BI.
    Args:
        gdf: GeoDataFrame with municipality geometries
        output_path: Path to save the TopoJSON file
    """
    try:
        # Simplify geometries for web display
        gdf_simplified = (gdf.to_crs(epsg=4326)
                             .copy())
        gdf_simplified["geometry"] = gdf_simplified.geometry.simplify(tolerance=0.02)

        # Set PBI identifier
        gdf_simplified["id"] = gdf_simplified["city_id"].astype(str)

        # Convert to TopoJSON
        topo = tp.Topology(gdf_simplified, prequantize=False)
        # Export as TopoJSON
        topo_json = topo.to_json()
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