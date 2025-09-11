# dea_analyzer/core.py
"""
DEA core module:
- Prepares input/output matrices
- Runs DEA models (CRS, VRS, IRS, DRS, input/output orientations)
- Computes scale efficiency and returns-to-scale classification
"""

import numpy as np
import pandas as pd
from dealib import RTS, Orientation, dea


class DEAAnalyzer:
    def __init__(self, inputs, outputs):
        """
        Initialize DEA Analyzer with chosen input and output fields.

        Args:
            inputs (list[str]): Column names for input variables.
            outputs (list[str]): Column names for output variables.
        """
        self.inputs = inputs
        self.outputs = outputs

    def _prepare_matrices(self, df):
        """Prepare input (X) and output (Y) matrices for DEA."""
        X = df[self.inputs].to_numpy()

        # Adjust outputs: IDEB stays, but for abandono we use (100 - taxa)
        y_ideb = df[["ideb_iniciais", "ideb_finais"]].to_numpy()
        abandono_iniciais = (100 - df["taxa_abandono_ef_anos_iniciais"]).to_numpy().reshape(-1, 1)
        abandono_finais = (100 - df["taxa_abandono_ef_anos_finais"]).to_numpy().reshape(-1, 1)

        Y = np.hstack([y_ideb, abandono_iniciais, abandono_finais])
        return X, Y

    def _run_models(self, X, Y):
        """Run CRS, VRS, IRS, DRS (input/output) models and return efficiencies."""
        models = {
            "crs_input": dea(X, Y, rts=RTS.crs, orientation=Orientation.input).eff,
            "crs_output": dea(X, Y, rts=RTS.crs, orientation=Orientation.output).eff,
            "vrs_input": dea(X, Y, rts=RTS.vrs, orientation=Orientation.input).eff,
            "vrs_output": dea(X, Y, rts=RTS.vrs, orientation=Orientation.output).eff,
            "irs_input": dea(X, Y, rts=RTS.irs, orientation=Orientation.input).eff,
            "drs_input": dea(X, Y, rts=RTS.drs, orientation=Orientation.input).eff,
        }

        # Adjust inverted output efficiencies
        models["crs_output"] = 1 / models["crs_output"]
        models["vrs_output"] = 1 / models["vrs_output"]

        # Scale efficiency (CRS/VRS input)
        models["scale_efficiency"] = models["crs_input"] / models["vrs_input"]

        # Returns to scale classification
        returns_nature = []
        for i in range(len(models["crs_input"])):
            if models["crs_input"][i] == models["vrs_input"][i]:
                returns_nature.append("Constante")
            elif models["drs_input"][i] == models["vrs_input"][i]:
                returns_nature.append("Decrescente")
            else:
                returns_nature.append("Crescente")
        models["returns_nature"] = returns_nature

        return models

    def run_all(self, df):
        """
        Run DEA for each year in the dataset.

        Args:
            df (pd.DataFrame): Input dataset with all years.

        Returns:
            dict: Results per year (original data + DEA scores).
            dict: Efficiency analysis per year (only scores).
        """
        results = {}
        efficiency_analysis = {}

        for year, subset in df.groupby("ano"):
            X, Y = self._prepare_matrices(subset)
            models = self._run_models(X, Y)

            # Build result DataFrame
            result_df = subset.copy()
            for name, values in models.items():
                if isinstance(values, (list, np.ndarray)) and len(values) == len(result_df):
                    result_df[f"DEA_{name}"] = values
            if "returns_nature" in models:
                result_df["DEA_returns_nature"] = models["returns_nature"]

            results[year] = {
                "models": models,
                "original_data": subset,
                "result_df": result_df
            }
            efficiency_analysis[year] = models

        return results, efficiency_analysis
