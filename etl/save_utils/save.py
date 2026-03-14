"""
Data saving utilities for local filesystem and Google Cloud Storage (GCS).

This module provides standardized methods to persist pandas DataFrames
and Python dictionaries in multiple formats — CSV, Parquet, and JSON —
both locally and remotely via GCS.
"""
import os
import tempfile
import json
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Any, Literal, Union
from google.cloud import storage
from dotenv import load_dotenv

def _convert_keys(obj: Any) -> Any:
    """
    Recursively convert dictionary keys to strings for JSON serialization.
    Args:
        obj (Any): Any object (dict, list, NumPy scalar, or other).
    Returns:
        Any: The object with all dictionary keys converted to strings.
    """
    if isinstance(obj, dict):
        return {str(k): _convert_keys(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_keys(i) for i in obj]
    if isinstance(obj, np.generic):
        return obj.item()
    return obj
    
def _save_to_file(obj: Union[pd.DataFrame, dict, str],
                  filepath: str | Path,
                  file_format: Literal["csv", "parquet", "json"],) -> None:
    """
    Save a DataFrame or dictionary to a local file.
    Args:
        obj: The DataFrame or dictionary.
        filepath: Full path including filename and extension.
        file_format: Output format ("csv", "parquet", or "json").
    Raises:
        ValueError: If an unsupported format is specified.
        TypeError: If the object type is unsupported.
    """
    if isinstance(obj,
                  pd.DataFrame):
        if file_format == "csv":
            obj.to_csv(filepath,
                       index = False)
        elif file_format == "parquet":
            obj.to_parquet(filepath,
                           index = False)
        elif file_format == "json":
            obj.to_json(filepath,
                        orient = "records",
                        indent = 2,
                        force_ascii = False)
        else:
            raise ValueError(f"Unsupported DataFrame format: {file_format}")
    elif isinstance(obj,
                    dict):
        if file_format != "json":
            raise ValueError("dictionary objects can only be saved as JSON")
        clean_obj = _convert_keys(obj)
        with open(filepath,
                  "w",
                  encoding = "utf-8") as f:
            json.dump(clean_obj,
                      f,
                      indent = 2,
                      ensure_ascii = False)
    elif isinstance(obj, str):
        if file_format != "json":
            raise ValueError("string objects can only be saved as JSON")
        with open(filepath, "w", encoding = "utf-8") as f:
            f.write(obj)
    else:
        raise TypeError("Only DataFrame or dict are supported")

def save_data(obj: Union[pd.DataFrame, dict, str],
              filename: str,
              directory: str = "data/raw",
              file_format: Literal["csv", "parquet", "json"] = "parquet",) -> str:
    """
    Save a DataFrame or dictionary to the local filesystem.
    Args:
        obj: The DataFrame or dictionary to save.
        filename: Filename without extension.
        directory: Target directory for saving (created if missing).
        file_format: Format to save in ("csv", "parquet", or "json").
    Returns:
        str: Full path of the saved file.
    """
    os.makedirs(directory,
                exist_ok = True)
    path = os.path.join(directory,
                        f"{filename}.{file_format}")
    _save_to_file(obj,
                  path,
                  file_format)
    return path

def save_data_to_gcs(obj: Union[pd.DataFrame, dict, str],
                     filename: str,
                     bucket_name: str,
                     layer: str = "bronze",
                     file_format: Literal["csv", "parquet", "json"] = "parquet",) -> str:
    """
    Save a DataFrame or dictionary to Google Cloud Storage (GCS).
    Requires the environment variable GOOGLE_APPLICATION_CREDENTIALS
    (optionally defined in a .env file).
    Args:
        obj: The DataFrame or dictionary to upload.
        filename: Filename without extension.
        bucket_name: Name of the target GCS bucket.
        layer: Subfolder within the bucket (default "bronze").
        file_format: Format to save in ("csv", "parquet", or "json").
    Returns:
        str: Path of the uploaded blob in GCS.
    Raises:
        RuntimeError: If the upload to GCS fails.
    """
    load_dotenv()
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    client = (storage.Client.from_service_account_json(cred_path) 
              if cred_path 
              else storage.Client())
    bucket = client.bucket(bucket_name)

    blob_path = f"{layer}/{filename}.{file_format}"

    tmp_file = os.path.join(tempfile.gettempdir(),
                            f"{filename}.{file_format}")
    try:
        _save_to_file(obj,
                      tmp_file,
                      file_format)
        bucket.blob(blob_path).upload_from_filename(tmp_file)
    except Exception as e:
        raise RuntimeError(f"Failed to upload {filename} to GCS: {e}")
    finally:
        if os.path.exists(tmp_file):
                          os.remove(tmp_file)
    return blob_path