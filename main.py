from etl.bronze_ingestion import bronze_ingestion
from etl import silver_processing
from etl import gold_etl

def main():
    """
    Entrypoint for DEApython project.
    Runs the complete ETL pipeline:
    1. Process data at bronze ingestion, silver processing and gold modeling
    2. Run statistical diagnostics
    3. Save results locally and to GCP
    """
    print(".:.Starting DEApython pipeline.:.")
    try:
        # Step 1: Bronze Ingestion
        print("Ingesting data...")
        bronze_ingestion()
        
        # Step 2: Silver Processing
        print("Processing data...")
        silver_processing.process_silver_data()
        
        # Step 3: Gold DEA Model
        print("Applying model...")
        gold_etl.process_gold_data()

        print("DEApython pipeline finished!")
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()