import os
import pandas as pd

def save_dataframe(df: pd.DataFrame, filename: str, directory: str = "data/raw", file_format: str = "parquet"):
    """
    Saves DataFrames to a specified directory with a specified file format.

    Parameters:
    - df: The pandas DataFrame to save.
    - filename: The name of the file (without extension).
    - directory: The directory where the file will be saved. Default is "data/raw".
    - file_format: The format to save the file in (default is "parquet").
    
    Returns:
    - None
    """
    # Create the directory if it doesn't exist
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Define the file path
    file_path = os.path.join(directory, f"{filename}.{file_format}")
    
    # Save the DataFrame in the specified format
    if file_format == "parquet":
        df.to_parquet(file_path, index=False)
    elif file_format == "csv":
        df.to_csv(file_path, index=False)
    elif file_format == "excel":
        df.to_excel(file_path, index=False)
    else:
        raise ValueError("Unsupported file format. Supported formats are 'parquet', 'csv', and 'excel'.")
    
    print(f"DataFrame saved to {file_path}")
