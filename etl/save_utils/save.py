import os
import numpy as np
import json
import pandas as pd
import tempfile
from typing import Union, Any, Dict
from google.cloud import storage
from dotenv import load_dotenv

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
    
def save_to_file(obj, filepath: str, file_format: str):
    """Internal helper to save DataFrame or dict to a file."""
    if isinstance(obj, pd.DataFrame):
        if file_format == "csv":
            obj.to_csv(filepath, index=False)
        elif file_format == "parquet":
            obj.to_parquet(filepath, index=False)
        elif file_format == "json":
            obj.to_json(filepath, orient="records", indent=2, force_ascii=False)
        else:
            raise ValueError(f"Unsupported DataFrame format: {file_format}")

    elif isinstance(obj, dict):
        if file_format != "json":
            raise ValueError("Dict objects can only be saved as JSON")
        clean_obj = convert_keys(obj)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(clean_obj, f, indent=2, ensure_ascii=False)

    else:
        raise TypeError("Only pandas.DataFrame or dict are supported")

def save_data(obj: Union[pd.DataFrame, Dict[Any, Any]],
              filename: str,
              directory: str = "data/raw",
              file_format: str = "parquet"
)-> str:
    """
    Save DataFrame or dictionary to local filesystem.
    
    Args:
        obj: pandas DataFrame or dictionary to save
        filename: Name without extension
        directory: Target directory
        file_format: File format ('parquet', 'csv', 'json')
    
    Raises:
        ValueError: Unsupported format or type mismatch
        TypeError: Unsupported object type
    """
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, f"{filename}.{file_format}")
    save_to_file(obj, path, file_format)
    return path

def save_data_to_gcs(
    obj: pd.DataFrame | dict,
    filename: str, 
    bucket_name: str, 
    layer: str = "bronze", 
    file_format: str = "parquet"
) -> str:
    """
    Save DataFrame or dictionary to Google Cloud Storage.
    
    Args:
        obj: pandas DataFrame or dictionary to save
        filename: Name without extension
        bucket_name: GCS bucket name
        layer: Folder layer in bucket (e.g., 'bronze', 'silver')
        file_format: File format ('parquet', 'csv', 'json')
    
    Returns:
        str: GCS blob path where file was uploaded
    
    Raises:
        RuntimeError: If upload to GCS fails
    """
    load_dotenv()
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    client = storage.Client.from_service_account_json(cred_path) if cred_path else storage.Client()
    bucket = client.bucket(bucket_name)

    blob_path = f"{layer}/{filename}.{file_format}"
    tmp_file = os.path.join(tempfile.gettempdir(), f"{filename}.{file_format}")

    try:
        save_to_file(obj, tmp_file, file_format)
        bucket.blob(blob_path).upload_from_filename(tmp_file)
    except Exception as e:
        raise RuntimeError(f"Failed to upload {filename} to GCS: {e}")
    finally:
        # Always clean up temp file
        if os.path.exists(tmp_file):
            os.remove(tmp_file)

    return blob_path
