from etl import bronze_ingestion
from etl import silver_processing
from etl import gold_modeling

def main():
    """
    Entrypoint for DEApython project.
    Runs the complete ETL pipeline:
    1. Process data at bronze ingestion, silver processing and gold DEA modeling
    2. Run statistical diagnostics, additional metrics
    3. Save results locally and to GCP in Medallion architecture
    """
    print(".:.Starting DEApython pipeline.:.")
    try:
        # Step 1: Bronze Ingestion
        print("Ingesting data...")
        bronze_ingestion.ingest_bronze_data()
        
        # Step 2: Silver Processing
        print("Processing data...")
        silver_processing.process_silver_data()
        
        # Step 3: Gold DEA Model
        print("Applying model...")
        gold_modeling.model_gold_data()

        print("DEApython pipeline finished!")
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()