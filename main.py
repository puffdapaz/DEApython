"""
Main entrypoint for DEApython project.

Runs the full pipeline:
1. ETL (gold data preparation)
2. DEA analysis
3. Save results
"""

from etl import run_gold_etl
from dea_analyzer import perform_dea_analysis
from save_utils import save_dataframe, save_summary


def main():
    print("🚀 Starting DEA pipeline...")

    # 1. Run ETL
    df = run_gold_etl()
    print(f"✅ ETL completed. Gold dataset shape: {df.shape}")

    # 2. DEA analysis
    results, efficiency_analysis = perform_dea_analysis()
    print("✅ DEA analysis completed.")

    # 3. Save results
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

    print("🏁 DEA pipeline finished successfully.")


if __name__ == "__main__":
    main()
