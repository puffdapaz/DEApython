# save_utils/save.py

import os
import pandas as pd
import tempfile
from google.cloud import storage
from dotenv import load_dotenv

def save_dataframe(df: pd.DataFrame, filename: str, directory: str = "data/output", file_format: str = "csv"):
    """
    Save a DataFrame to the specified directory with given format.
    Automatically creates directory if it doesn't exist.
    """
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, f"{filename}.{file_format}")
    if file_format == "csv":
        df.to_csv(path, index=False)
    elif file_format == "xlsx":
        df.to_excel(path, index=False)
    else:
        raise ValueError(f"Unsupported format: {file_format}")
    print(f"💾 Saved: {path}")

def save_dataframe_to_gcs(
    df: pd.DataFrame, 
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

    # Save locally first
    if file_format == "parquet":
        df.to_parquet(tmp_file, index=False)
    elif file_format == "csv":
        df.to_csv(tmp_file, index=False)
    elif file_format == "excel":
        df.to_excel(tmp_file, index=False)
    else:
        raise ValueError("Unsupported file format. Supported: parquet, csv, excel.")

    # Upload to GCS
    blob.upload_from_filename(tmp_file)

    print(f"✅ Uploaded {filename}.{file_format} to GCP://{bucket_name}/{layer}/")


def save_summary(df: pd.DataFrame, filename: str, directory: str = "data/output", file_format: str = "csv"):
    """
    Save summary statistics of a DataFrame.
    Wrapper around save_dataframe.
    """
    # e.g. df.describe() if needed outside
    save_dataframe(df, filename, directory=directory, file_format=file_format)
