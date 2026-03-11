from etl import bronze_ingestion
from etl import silver_processing
from etl import gold_modeling
from etl.save_utils.load_to_db import load_to_neon
from openmetadata import metadata_register

def main():
    """
    Entrypoint for DEApython project.
    Runs the complete pipeline:
    1. Register metadata in OpenMetadata
    2. Fetch raw data at bronze layer and saved to GCS
    3. Aggregate data and geodata at silver layer and saved to GCS
    4. DEA modelling, diagnostics and additional analysis metrics 
       applied at gold layer and saved to GCS
    5. Load final results to Neon PostgreSQL DataWarehouse
    """
    print(".:. Starting DEApython pipeline .:.")
    try:
        # Step 1: OpenMetadata Registration
        print("Registering metadata...")
        metadata_register.metadata_setup()

        # Step 2: Bronze Ingestion
        print("Ingesting data...")
        bronze_ingestion.ingest_bronze_data()
        
        # Step 3: Silver Processing
        print("Processing data...")
        silver_processing.process_silver_data()
        
        # Step 4: Gold DEA Model
        print("Applying model...")
        dea_data = gold_modeling.model_gold_data()
        
        # Step 5: Load to DataWarehouse
        print("Loading data...")
        load_to_neon(dea_data)

        print("DEApython pipeline finished!")
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()