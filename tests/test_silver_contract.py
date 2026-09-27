import importlib


def test_silver_contract_module_imports():
    """Guards the bug class that broke validators.py: a module that cannot be
    imported fails silently inside run_etl_pipeline.py's blanket except."""
    importlib.import_module("lib.classes.silver_contract")


def test_validators_module_imports():
    importlib.import_module("lib.classes.validators")
