# etl/gold_dea.py
"""
Gold layer ETL script:
- Loads processed (silver) data
- Runs DEA analysis using DEAAnalyzer
- Exports results and summaries
"""

import pandas as pd
from dea_analyzer.core import DEAAnalyzer
from dea_analyzer.diagnostics import run_diagnostics
from save_utils.save import save_dataframe

# Configs: you could replace with configs/dea_config.yml
INPUT_PATH = "data/processed/gold/gold_data_complete_cases.csv"
OUTPUT_DIR = "data/output"

def run_gold_stage():
    # 1. Load silver/processed data
    print("📂 Loading data from silver stage...")
    complete_cases = pd.read_csv(INPUT_PATH)

    # 2. Initialize DEA analyzer
    analyzer = DEAAnalyzer(
        inputs=["pib_per_capita", "gasto_por_aluno"],
        outputs=[
            "ideb_iniciais", "ideb_finais",
            "taxa_abandono_ef_anos_iniciais", "taxa_abandono_ef_anos_finais"
        ]
    )

    # 3. Run DEA for each year
    print("⚙️ Running DEA analysis...")
    results, efficiency_analysis = analyzer.run_all(complete_cases)

    # 4. Run diagnostics (normality, KS, scale efficiency)
    print("🔍 Running diagnostics...")
    diagnostics = run_diagnostics(efficiency_analysis)

    # 5. Export results
    print("💾 Exporting results...")
    for year, year_data in results.items():
        if "result_df" not in year_data:
            continue
        df = year_data["result_df"]

        # Save full result
        save_dataframe(df, f"DEA_Analysis_{year}", directory=OUTPUT_DIR, file_format="csv")

        # Save summary
        dea_cols = [c for c in df.columns if c.startswith("DEA_")]
        if dea_cols:
            summary = df[dea_cols].describe()
            save_dataframe(summary, f"DEA_Summary_{year}", directory=OUTPUT_DIR, file_format="csv")

    # Optionally: save diagnostics report
    save_dataframe(pd.DataFrame(diagnostics), "DEA_Diagnostics", directory=OUTPUT_DIR, file_format="csv")

    print("✅ Gold stage completed!")


if __name__ == "__main__":
    run_gold_stage()
