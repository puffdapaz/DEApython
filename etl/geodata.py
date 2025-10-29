"""
Geodata extraction and merging utilities.
This module integrates Brazilian municipality geospatial data
from the geobr database into the Silver layer dataset.
"""

import logging
import geopandas as gpd
import geobr
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        gdf = gpd.GeoDataFrame(gdf).drop(columns = ["name_muni",
                                                    "code_state",
                                                    "abbrev_state"])
        return gdf
    except Exception as e:
        logging.error(f"Error fetching geobr data: {e}")
        raise

def merge_geodata(data : pd.DataFrame,
                  geodata : gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Merge the analytical dataset with municipality geodata.
    Args:
        data (pd.DataFrame): Silver-layer analytical dataset with 'city_id' column.
        geodata (gpd.GeoDataFrame): Municipality polygons from geobr.
    Returns:
        gpd.GeoDataFrame: Enriched dataset with geometries.
    Raises:
        Exception: If merging geodata fails.
    """
    try:
        df = data.copy()
        df["city_id_int"] = df["city_id"].astype(str).astype(int)
        geodata["code_muni"] = geodata["code_muni"].astype(int)

        merged_map = df.merge(geodata[["code_muni", "geometry"]],
                              how="left",
                              left_on="city_id_int",
                              right_on="code_muni")
        merged_map = merged_map.drop(columns=["city_id_int", "code_muni"])

        merged_map = gpd.GeoDataFrame(merged_map,
                                      geometry = "geometry",
                                      crs = geodata.crs)
        merged_map["geometry"] = merged_map.geometry.simplify(tolerance = 0.01)
        print("geobr data merging completed")
        return merged_map
    except Exception as e:
        logging.error(f"Error merging geobr data: {e}")
        raise