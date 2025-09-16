"""
Diagnostics and statistical tests for DEA analysis
"""

from .model_diagnostics import (
    shapiro_wilk_test,
    kolmogorov_smirnov_test,
    efficiency_normality_test,
    returns_to_scale_test,
    scale_efficiency_test
)

from .data_validation import (
    load_configs,
    validate_bronze_data,
    validate_silver_data,
    validate_gold_data
)

__all__ = [
    'shapiro_wilk_test',
    'kolmogorov_smirnov_test', 
    'efficiency_normality_test',
    'returns_to_scale_test',
    'scale_efficiency_test',
    'validate_dea_results',
    'check_efficiency_bounds',
    'validate_returns_to_scale',
    'generate_diagnostics_report'
]