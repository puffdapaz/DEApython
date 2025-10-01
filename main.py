
from etl.bronze_ingestion import bronze_ingestion
from etl import silver_processing
from etl import gold_etl

"""
Entrypoint for DEApython project.
Runs the complete ETL pipeline:
1. Process bronze, silver, gold data
2. Run DEA analysis
3. Save results
"""

def main():
    """Main pipeline execution function"""
    print(".:.Starting DEApython Pipeline.:.")
    
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

        print("DEApython workflow completed!")
        
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()