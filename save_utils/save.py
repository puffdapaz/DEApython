# save_utils/save.py

import os
import pandas as pd

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

def save_summary(df: pd.DataFrame, filename: str, directory: str = "data/output", file_format: str = "csv"):
    """
    Save summary statistics of a DataFrame.
    Wrapper around save_dataframe.
    """
    # e.g. df.describe() if needed outside
    save_dataframe(df, filename, directory=directory, file_format=file_format)
