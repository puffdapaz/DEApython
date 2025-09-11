import pandas as pd
import numpy as np
from dealib import RTS, Orientation, dea
from scipy import stats
from save import save_dataframe


def perform_dea_analysis():
    # Load data
    complete_cases = pd.read_csv("data/processed/gold/gold_data_complete_cases.csv")

    # Descriptive statistics
    print("Data Description:")
    print(complete_cases.groupby('ano').describe().stack())
    print(complete_cases.groupby('ano').corr(numeric_only=True))

    results = {}
    efficiency_analysis = {}

    def prepare_matrices(df):
        X = df[['pib_per_capita', 'gasto_por_aluno']].to_numpy()

        # Outputs: IDEB + (100 - abandono) as proxy for "good" output
        y_ideb = df[['ideb_iniciais', 'ideb_finais']].to_numpy()
        abandono_iniciais = (100 - df['taxa_abandono_ef_anos_iniciais']).to_numpy().reshape(-1, 1)
        abandono_finais = (100 - df['taxa_abandono_ef_anos_finais']).to_numpy().reshape(-1, 1)

        Y = np.hstack([y_ideb, abandono_iniciais, abandono_finais])

        print(f"Input shape: {X.shape}, Output shape: {Y.shape}")
        print(f"Input range: [{X.min():.2f}, {X.max():.2f}]")
        print(f"Output range: [{Y.min():.2f}, {Y.max():.2f}]")

        return X, Y

    for year, subset in complete_cases.groupby("ano"):
        print(f"\nRunning DEA for year {year}")
        X, Y = prepare_matrices(subset)

        dea_models = {}
        dea_models['crs_input'] = dea(X, Y, rts=RTS.crs, orientation=Orientation.input).eff
        dea_models['crs_output'] = dea(X, Y, rts=RTS.crs, orientation=Orientation.output).eff
        dea_models['vrs_input'] = dea(X, Y, rts=RTS.vrs, orientation=Orientation.input).eff
        dea_models['vrs_output'] = dea(X, Y, rts=RTS.vrs, orientation=Orientation.output).eff
        dea_models['irs_input'] = dea(X, Y, rts=RTS.irs, orientation=Orientation.input).eff
        dea_models['drs_input'] = dea(X, Y, rts=RTS.drs, orientation=Orientation.input).eff

        # Efficiency scores mapping
        eff_scores = {
            "RCOI": dea_models['crs_input'],
            "RCOP": dea_models['crs_output'],
            "RVOI": dea_models['vrs_input'],
            "RVOP": dea_models['vrs_output'],
            "RNDOI": dea_models['irs_input'],
            "RNCOI": dea_models['drs_input']
        }

        eff_scores["RCOP"] = 1 / eff_scores["RCOP"]
        eff_scores["RVOP"] = 1 / eff_scores["RVOP"]

        # Scale efficiency
        if "RCOI" in eff_scores and "RVOI" in eff_scores:
            eff_scores["scale_efficiency"] = eff_scores["RCOI"] / eff_scores["RVOI"]

        # Normality & KS tests
        print(f"Shapiro-Wilk VRS-input {year}: {stats.shapiro(eff_scores['RVOI'])}")
        print(f"KS CRS vs VRS-input {year}: {stats.ks_2samp(eff_scores['RCOI'], eff_scores['RVOI'])}")

        # Returns to scale nature
        returns_nature = []
        if "RCOI" in eff_scores and "RVOI" in eff_scores and "RNCOI" in eff_scores:
            for i in range(len(eff_scores["RCOI"])):
                if eff_scores["RCOI"][i] == eff_scores["RVOI"][i]:
                    returns_nature.append("Constante")
                elif eff_scores["RNCOI"][i] == eff_scores["RVOI"][i]:
                    returns_nature.append("Decrescente")
                else:
                    returns_nature.append("Crescente")
            eff_scores["returns_nature"] = returns_nature

        # Build result dataframe
        result_df = subset.copy()
        for score_name, scores in eff_scores.items():
            if score_name != "returns_nature" and len(scores) == len(result_df):
                result_df[f"DEA_{score_name}"] = scores
        if "returns_nature" in eff_scores:
            result_df["DEA_returns_nature"] = eff_scores["returns_nature"]

        results[year] = {
            "models": dea_models,
            "original_data": subset,
            "result_df": result_df
        }
        efficiency_analysis[year] = eff_scores

    export_dea_results(results)

    print(f"✅ Year {year} final efficiency ranges:")
    for score_name, scores in eff_scores.items():
        print(f"  {score_name}: [{scores.min():.3f}, {scores.max():.3f}]")

    return results, efficiency_analysis


def export_dea_results(results):
    for year, year_data in results.items():
        if "result_df" not in year_data:
            continue
        df = year_data["result_df"]

        if "DEA_RCOI" in df.columns and "DEA_RVOI" in df.columns:
            df["DEA_scale_efficiency"] = df["DEA_RCOI"] / df["DEA_RVOI"]

         # Save to CSV
        save_dataframe(
            df, 
            f"DEA_Analysis_{year}", 
            directory="data/output", 
            file_format="csv"
        )

        dea_cols = [c for c in df.columns if c.startswith("DEA_")]
        if dea_cols:
            save_dataframe(df[dea_cols].describe(), f"DEA_Summary_{year}", directory="data/output", file_format="csv")
        print(f"✅ Exported results for {year}")

if __name__ == "__main__":
    results, efficiency_analysis = perform_dea_analysis()
    if results and efficiency_analysis:
        print("✅ DEA analysis completed successfully!")

        # Print some summary information
        for year in results.keys():
            if year in efficiency_analysis:
                print(f"\nYear {year} Summary:")
                print(f"  Number of DMUs: {len(results[year]['result_df'])}")