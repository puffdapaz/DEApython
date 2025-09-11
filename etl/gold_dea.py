# etl/gold_dea.py

import os
import yaml
import pandas as pd
from dea_analyzer.core import DEAAnalyzer
from dea_analyzer.diagnostics import run_diagnostics
from save_utils.save import save_dataframe

def load_configs():
    """Load DEA and path configurations from YAML files."""
    with open("configs/dea_config.yml", "r") as f:
        dea_config = yaml.safe_load(f)
    with open("configs/path.yml", "r") as f:
        paths = yaml.safe_load(f)
    return dea_config, paths

def run_gold_stage():
    # Load configuration
    dea_config, paths = load_configs()

    # Prepare file paths from configuration
    input_file = paths['files']['gold_input']
    output_dir = paths['data']['output']

    # Load data
    print("📂 Loading data...")
    complete_cases = pd.read_csv(input_file)

    # Descriptive statistics (optional)
    print("Data Description:")
    print(complete_cases.groupby('ano').describe().stack())
    print(complete_cases.groupby('ano').corr(numeric_only=True))

    # Extract variables from config
    input_vars = dea_config.get('input_vars',
                                ["pib_per_capita", "gasto_por_aluno"])
    output_vars = dea_config.get('output_vars',
                                 ["ideb_iniciais", "ideb_finais",
                                  "taxa_abandono_ef_anos_iniciais",
                                  "taxa_abandono_ef_anos_finais"])

    # Initialize DEA analyzer
    analyzer = DEAAnalyzer(inputs=input_vars, outputs=output_vars)

    # Run DEA
    print("⚙️ Running DEA analysis by year...")
    results, efficiency_analysis = analyzer.run_all(complete_cases)

    # Run diagnostics
    print("🔍 Running diagnostics...")
    diagnostics = run_diagnostics(efficiency_analysis)

    # Export results
    print("💾 Exporting DEA results and summaries...")
    for year, year_data in results.items():
        result_df = year_data['result_df']
        save_dataframe(result_df, f"DEA_Analysis_{year}", directory=output_dir, file_format="csv")

        dea_cols = [c for c in result_df.columns if c.startswith("DEA_")]
        if dea_cols:
            summary_df = result_df[dea_cols].describe()
            save_dataframe(summary_df, f"DEA_Summary_{year}", directory=output_dir, file_format="csv")

    # Export diagnostics
    # Convert diagnostics (which maybe a nested dict) to DataFrame
    diag_list = []
    for year, diag in diagnostics.items():
        row = {'year': year}
        for test_name, result in diag.items():
            # for Shapiro or KS, we can store statistic & pvalue
            row[f"{test_name}_stat"] = getattr(result, 'statistic', None)
            row[f"{test_name}_pvalue"] = getattr(result, 'pvalue', None)
        diag_list.append(row)
    diag_df = pd.DataFrame(diag_list)
    save_dataframe(diag_df, "DEA_Diagnostics", directory=output_dir, file_format="csv")

    print("✅ Gold stage completed!")


if __name__ == "__main__":
    run_gold_stage()
