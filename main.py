from etl import bronze_ingestion
from etl import silver_processing
from etl import gold_modeling
from etl.save_utils.load_to_db import load_to_neon
from openmetadata import metadata_register

def main():
    """
    Entrypoint for DEApython project.
    Runs the complete ETL pipeline:
    1. Process data at bronze ingestion, silver processing and gold DEA modeling
    2. Run statistical diagnostics, additional metrics
    3. Save results locally and to GCP in Medallion architecture
    4. Load final results to Neon DataWarehouse
    5. Register metadata in OpenMetadata
    """
    print(".:. Starting DEApython pipeline .:.")
    try:
        # Step 1: Bronze Ingestion
        print("Ingesting data...")
        bronze_ingestion.ingest_bronze_data()
        
        # Step 2: Silver Processing
        print("Processing data...")
        silver_processing.process_silver_data()
        
        # Step 3: Gold DEA Model
        print("Applying model...")
        dea_data = gold_modeling.model_gold_data()
        
        # Step 4: Load to DataWarehouse
        print("Loading data...")
        load_to_neon(dea_data)

        # Step 5: OpenMetadata Registration
        print("Registering metadata...")
        metadata_register.main()

        print("DEApython pipeline finished!")
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()