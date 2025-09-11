# tests/test_imports.py
"""
Quick smoke test for verifying imports and function links in DEApython repo.
Run with: pytest tests/test_imports.py
"""

import pytest

def test_import_save_utils():
    from save_utils import save_dataframe, save_summary
    assert callable(save_dataframe)
    assert callable(save_summary)

def test_import_gold_dea():
    from etl import gold_dea
    assert hasattr(gold_dea, "run_gold_etl")

def test_import_dea_analyzer():
    from dea_analyzer import analyzer
    assert hasattr(analyzer, "perform_dea_analysis")

def test_import_main():
    import main
    assert hasattr(main, "main")
