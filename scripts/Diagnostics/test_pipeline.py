"""
Test script for the DEA pipeline.
Runs ETL → DEA analysis → Saves results.
"""

from etl import run_gold_etl
from dea_analyzer import perform_dea_analysis
from save_utils import save_dataframe, save_summary


def test_pipeline():
    # 1. Run ETL (creates processed gold dataset)
    df = run_gold_etl()
    print("✅ ETL finished. Shape:", df.shape)

    # 2. Run DEA analysis
    results, efficiency_analysis = perform_dea_analysis()
    print("✅ DEA analysis completed.")

    # 3. Save diagnostic summaries
    for year, year_data in results.items():
        if "result_df" in year_data:
            save_dataframe(
                year_data["result_df"],
                f"DEA_Full_{year}",
                directory="data/output",
                file_format="csv"
            )
            save_summary(
                year_data["result_df"],
                f"DEA_Summary_{year}",
                directory="data/output",
                file_format="csv"
            )
            print(f"📂 Results exported for {year}")


if __name__ == "__main__":
    test_pipeline()
