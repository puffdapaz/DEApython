# save_utils/save.py
"""
Helper functions to save DataFrames to CSV, Excel, etc.
"""

import os


def save_dataframe(df, filename, directory="data/output", file_format="csv"):
    os.makedirs(directory, exist_ok=True)

    path = os.path.join(directory, f"{filename}.{file_format}")
    if file_format == "csv":
        df.to_csv(path, index=False)
    elif file_format == "xlsx":
        df.to_excel(path, index=False)
    else:
        raise ValueError(f"Unsupported format: {file_format}")

    print(f"💾 Saved: {path}")
