# etl/__init__.pyy
from etl.bronze_ingestion import ingest_bronze_data
from etl.silver_processing import process_silver_data
from etl.gold_modeling import model_gold_data

__all__ = ['ingest_bronze_data',
           'process_silver_data',
           'model_gold_data'
]
__version__ = "0.1.0"

