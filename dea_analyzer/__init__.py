# dea_analyzer/__init__.py
# Core DEA package
from .core import prepare_matrices, run_dea_models, build_efficiency_dataframe
from .diagnostics import shapiro_test, ks_test
from .analyzer import perform_dea_analysis

__all__ = ["perform_dea_analysis"]