import importlib
from datetime import date
from decimal import Decimal

import pytest


def test_silver_contract_module_imports():
    """Guards the bug class that broke validators.py: a module that cannot be
    imported fails silently inside run_etl_pipeline.py's blanket except."""
    importlib.import_module("lib.classes.silver_contract")


def test_validators_module_imports():
    importlib.import_module("lib.classes.validators")


def _row(**over):
    """A minimal valid Silver row, matching the canonical column names."""
    row = {
        "area_name_en": "Dubai Marina",
        "area_name_ar": "دبي مارينا",
        "property_usage_en": "Residential",
        "ejari_property_type_en": "Unit",
        "ejari_property_sub_type_en": "Flat",
        "project_name_en": "Ocean Heights",
        "master_project_en": None,
        "nearest_metro_en": "DMCC Metro Station",
        "nearest_mall_en": "Dubai Marina Mall",
        "nearest_landmark_en": None,
        "property_type_ar": "وحدة",
        "property_sub_type_ar": "شقه",
        "property_usage_ar": "سكني",
        "project_ar": "أوشن هايتس",
        "nearest_metro_ar": "محطة مترو",
        "nearest_mall_ar": "دبي مارينا مول",
        "nearest_landmark_ar": None,
        "contract_registration_date": date(2026, 9, 12),
        "contract_start_date": date(2026, 9, 20),
        "contract_end_date": date(2027, 9, 19),
        "version_en": "New",
        "is_free_hold": True,
        "annual_amount": Decimal("225000"),
        "contract_amount": Decimal("225000"),
        "actual_area": Decimal("1450"),
        "rooms": 2,
        "has_parking": True,
        "total_properties": 1,
    }
    row.update(over)
    return row


def test_model_constructs_from_a_silver_row():
    from lib.classes.silver_contract import SilverRentContract

    c = SilverRentContract(**_row())
    assert c.area_name_en == "Dubai Marina"
    assert c.annual_amount == Decimal("225000")
    assert c.is_free_hold is True


def test_field_on_a_leading_underscore_name_is_an_error():
    """The form that broke BronzeRentContract was Field(default_factory=...);
    a plain Field(default=...) raises identically. Both are the loud form. A
    bare default or a bare annotation is accepted silently as a private
    attribute, which is why only the Field() form raised."""
    from pydantic import BaseModel, Field

    with pytest.raises(NameError, match="leading underscores"):

        class Bad(BaseModel):
            _private: str = Field(default="x")


def test_shipped_model_declares_no_private_attributes():
    """Catches all four silent forms, including the two that raise no error at
    all. A stray private attribute means a real field was mis-declared and the
    column is being discarded by extra='ignore'."""
    from lib.classes.silver_contract import SilverRentContract

    assert SilverRentContract.__private_attributes__ == {}
    assert SilverRentContract.model_fields["area_name_en"].is_required()


def test_all_canonical_names_are_declared():
    """A name present in the row payload but absent from the model is discarded
    by extra='ignore' and surfaces later as an AttributeError far from the
    omission. master_project_en is read by dim_project.sql:10,18,25."""
    from lib.classes.silver_contract import SilverRentContract

    for name in (
        "area_name_en", "annual_amount", "actual_area", "contract_start_date",
        "property_usage_en", "project_name_en", "master_project_en",
        "ejari_property_type_en", "ejari_property_sub_type_en",
        "contract_registration_date", "contract_amount",
    ):
        assert name in SilverRentContract.model_fields, name
