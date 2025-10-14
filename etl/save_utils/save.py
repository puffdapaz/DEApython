"""
Data saving utilities for local filesystem and Google Cloud Storage.

This module provides standardized methods to persist dataframes
and dictionaries in multiple formats, both locally and remotely.
"""
import os
import tempfile
import json
import numpy as np
import pandas as pd
from typing import Any
from google.cloud import storage
from dotenv import load_dotenv

def _convert_keys(obj: Any) -> Any:
    """
    Recursively convert dictionary keys to str (for JSON serialization).
    Args:
        obj: Any object (dict, list, numpy scalar, or other).
    Returns:
        The object with all dictionary keys converted to str.
    """
    if isinstance(obj,
                  dict):
        return {str(k): _convert_keys(v) for k,
                                      v in obj.items()}
    elif isinstance(obj,
                    list):
        return [_convert_keys(i) for i in obj]
    elif isinstance(obj,
                    np.generic):
        return obj.item()
    else:
        return obj
    
def _save_to_file(obj: pd.DataFrame | dict,
                  filepath: str,
                  file_format: str) -> None:
    """
    Save a DataFrame or dictionary to a file.
    Args:
        obj: DataFrame or dictionary to save.
        filepath: Full path including filename and extension.
        file_format: Format to save in ("csv", "parquet", "json").
    Raises:
        ValueError: If unsupported format is used.
        TypeError: If object type is not supported.
    """
    if isinstance(obj,
                  pd.DataFrame):
        if file_format == "csv":
            obj.to_csv(filepath,
                       index=False)
        elif file_format == "parquet":
            obj.to_parquet(filepath,
                           index=False)
        elif file_format == "json":
            obj.to_json(filepath,
                        orient="records",
                        indent=2,
                        force_ascii=False)
        else:
            raise ValueError(f"Unsupported DataFrame format: {file_format}")
    elif isinstance(obj,
                    dict):
        if file_format != "json":
            raise ValueError("Dict objects can only be saved as JSON")
        clean_obj = _convert_keys(obj)
        with open(filepath,
                  "w",
                  encoding="utf-8") as f:
            json.dump(clean_obj,
                      f,
                      indent=2,
                      ensure_ascii=False)
    else:
        raise TypeError("Only DataFrame or dict are supported")

def save_data(obj: pd.DataFrame | dict,
              filename: str,
              directory: str = "data/raw",
              file_format: str = "parquet") -> str:
    """
    Save a DataFrame or dictionary to the local filesystem.
    Args:
        obj: DataFrame or dictionary to save.
        filename: Name without extension.
        directory: Target directory for saving.
        file_format: Format to save in ("csv", "parquet", "json").
    Returns:
        str: Path of the saved file.
    """
    os.makedirs(directory,
                exist_ok=True)
    path = os.path.join(directory,
                        f"{filename}.{file_format}")
    _save_to_file(obj,
                 path,
                 file_format)
    return path

def save_data_to_gcs(obj: pd.DataFrame | dict,
                     filename: str, 
                     bucket_name: str, 
                     layer: str = "bronze", 
                     file_format: str = "parquet") -> str:
    """
    Save a DataFrame or dictionary to Google Cloud Storage (GCS).
    Requires GOOGLE_APPLICATION_CREDENTIALS to be set in .env or environment.
    Args:
        obj: DataFrame or dictionary to save.
        filename: Name without extension.
        bucket_name: Target GCS bucket name.
        layer: Folder layer within the bucket (default "bronze").
        file_format: Format to save in ("csv", "parquet", "json").
    Returns:
        str: GCS blob path where the file was uploaded.
    Raises:
        RuntimeError: If upload to GCS fails.
    """
    load_dotenv()
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    client = storage.Client.from_service_account_json(cred_path) if cred_path else storage.Client()
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