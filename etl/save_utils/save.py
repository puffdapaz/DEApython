# save_utils/save.py

import os
import numpy as np
import json
import pandas as pd
import tempfile
from google.cloud import storage
from dotenv import load_dotenv

import json

def convert_keys(obj):
    """Recursively convert dict keys to str (for JSON serialization)."""
    if isinstance(obj, dict):
        return {str(k): convert_keys(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_keys(i) for i in obj]
    elif isinstance(obj, np.generic):  # handles np.int64, np.float32 etc.
        return obj.item()
    else:
        return obj

def save_dataframe(obj, filename: str, directory: str = "data/raw", file_format: str = "csv"):
    """
    Save a DataFrame or dict to the specified directory with given format.
    - Supports DataFrame (csv, xlsx, json)
    - Supports dict (json)
    """
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, f"{filename}.{file_format}")

    if isinstance(obj, pd.DataFrame):
        if file_format == "csv":
            obj.to_csv(path, index=False)
        elif file_format == "parquet":
            obj.to_parquet(path, index=False)
        elif file_format == "json":
            obj.to_json(path, orient="records", indent=2, force_ascii=False)
        else:
            raise ValueError(f"Unsupported format for DataFrame: {file_format}")

    elif isinstance(obj, dict):
        if file_format != "json":
            raise ValueError("Dict objects can only be saved as JSON")

        clean_obj = convert_keys(obj)  # 🔑 convert numpy keys + values
        with open(path, "w", encoding="utf-8") as f:
            json.dump(clean_obj, f, indent=2, ensure_ascii=False)

    else:
        raise TypeError("save_dataframe only supports pandas.DataFrame or dict")

def save_dataframe_to_gcs(
    obj,
    filename: str, 
    bucket_name: str, 
    layer: str = "bronze", 
    file_format: str = "parquet"
):
    """
    Saves a Pandas DataFrame into a GCS bucket under a given 'layer' folder.
    """
    load_dotenv()
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    client = storage.Client.from_service_account_json(cred_path)
    bucket = client.bucket(bucket_name)

    # Blob path → layer/filename.format
    blob_path = f"{layer}/{filename}.{file_format}"
    blob = bucket.blob(blob_path)

    # Use cross-platform temp directory
    tmp_dir = tempfile.gettempdir()
    tmp_file = os.path.join(tmp_dir, f"{filename}.{file_format}")

    if isinstance(obj, pd.DataFrame):
        if file_format == "parquet":
            obj.to_parquet(tmp_file, index=False)
        elif file_format == "csv":
            obj.to_csv(tmp_file, index=False)
        elif file_format == "excel":
            obj.to_excel(tmp_file, index=False)
        else:
            raise ValueError("Unsupported file format for DataFrame. Supported: parquet, csv, excel.")
    
    elif isinstance(obj, dict):
        if file_format != "json":
            raise ValueError("Dict objects can only be saved as JSON")
        clean_obj = convert_keys(obj)
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(clean_obj, f, indent=2, ensure_ascii=False)

    else:
        raise TypeError("save_dataframe_to_gcs only supports pandas.DataFrame or dict")
    # Upload to GCS
    blob.upload_from_filename(tmp_file)
