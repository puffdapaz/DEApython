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

        # Outputs: IDEB and (100 - abandono) as proxy for "good" output
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

        # dea() default
        # dea(X, Y, RTS="", ORIENTATION="")

        # X = Input matrix from sampled municipalities;
        # Y = Output matrix from sampled municipalities;
        # RTS = Returns to Scale: Constant, Variable, Increasing or Decreasing;
        # ORIENTATION: Orientation: input or output.

        # Data Envelopment Analysis Efficiency Estimation:
        dea_models = {}
        # Constant Returns to Scale Input Oriented 
        dea_models['crs_input'] = dea(X, Y, rts=RTS.crs, orientation=Orientation.input).eff
        # Constant Returns to Scale Output Oriented
        dea_models['crs_output'] = dea(X, Y, rts=RTS.crs, orientation=Orientation.output).eff
        # Variable Returns to Scale Input Oriented
        dea_models['vrs_input'] = dea(X, Y, rts=RTS.vrs, orientation=Orientation.input).eff
        # Variable Returns to Scale Output Oriented
        dea_models['vrs_output'] = dea(X, Y, rts=RTS.vrs, orientation=Orientation.output).eff
        # Increasing Returns to Scale Input Oriented
        dea_models['irs_input'] = dea(X, Y, rts=RTS.irs, orientation=Orientation.input).eff
        # Decreasing Returns to Scale Input Oriented
        dea_models['drs_input'] = dea(X, Y, rts=RTS.drs, orientation=Orientation.input).eff

        # Efficiency scores mapping
        eff_scores = {
            "crs_input": dea_models['crs_input'],
            "crs_output": dea_models['crs_output'],
            "vrs_input": dea_models['vrs_input'],
            "vrs_output": dea_models['vrs_output'],
            "irs_input": dea_models['irs_input'],
            "drs_input": dea_models['drs_input']
        }

        eff_scores['crs_output'] = 1 / eff_scores['crs_output']
        eff_scores['vrs_output'] = 1 / eff_scores['vrs_output']

        # Normality & KS tests
        print(f"Shapiro-Wilk VRS-input {year}: {stats.shapiro(eff_scores['vrs_input'])}")
        print(f"KS CRS vs VRS-input {year}: {stats.ks_2samp(eff_scores['crs_input'], eff_scores['vrs_input'])}")
        # Ho: Absence of scale inefficiency;
        # (the model with the assumption of constant returns is the most appropriate);
        # Hi: Presence of scale inefficiency;
        # (the model with the assumption of variable returns is the most appropriate).

        # In educational performance, the inputs considered are resources such as funding, teachers, and facilities,
        # while the outputs are typically student performance measures such as test scores, graduation rates, and college enrollment.
        # Hence, a resource orientation is most appropriate in this context.

        # Scale efficiency
        if 'crs_input' in eff_scores and 'vrs_input' in eff_scores:
            eff_scores["scale_efficiency"] = eff_scores['crs_input'] / eff_scores['vrs_input']

        # Returns to scale nature
        returns_nature = []
        if 'crs_input' in eff_scores and 'vrs_input' in eff_scores and 'drs_input' in eff_scores:
            for i in range(len(eff_scores['crs_input'])):
                if eff_scores['crs_input'][i] == eff_scores['vrs_input'][i]:
                    returns_nature.append("Constante")
                elif eff_scores['drs_input'][i] == eff_scores['vrs_input'][i]:
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

    return results, efficiency_analysis


def export_dea_results(results):
    for year, year_data in results.items():
        if "result_df" not in year_data:
            continue
        df = year_data["result_df"]

        if "DEA_crs_input" in df.columns and "DEA_vrs_input" in df.columns:
            df["DEA_scale_efficiency"] = df["DEA_crs_input"] / df["DEA_vrs_input"]

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