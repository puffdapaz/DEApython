# src/dea_education/__init__.py
from bronze_ingestion import bronze_ingestion
from silver_processing import process_silver_data
from gold_etl import process_gold_data
from .save_utils import save_dataframe, save_dataframe_to_gcs

__version__ = "0.1.0"