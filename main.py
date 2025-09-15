"""
Main entrypoint for DEApython project.

Runs the full pipeline:
1. ETL (gold data preparation)
2. DEA analysis
3. Save results
"""
from .etl import bronze_ingestion, silver_processing, gold_etl
from .etl.save_utils import save_dataframe, save_dataframe_to_gcs


def main():
    print("🚀 Starting DEA pipeline...")

    # 1. Run Bronze fetch
    df_bronze = bronze_ingestion()
    print(f"✅ Fetch completed")

    # 2. Run Silver processing
    df_silver = process_silver_data()
    print("✅ Processing completed.")

    # 3. Run Gold finishing
    df_gold = process_gold_data()
    print("✅ Processing completed.")

if __name__ == "__main__":
    main()
