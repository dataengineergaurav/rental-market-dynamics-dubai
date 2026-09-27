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


def test_derives_duration_and_short_term_flag():
    from lib.classes.silver_contract import SilverRentContract

    c = SilverRentContract(**_row())
    assert c.duration_days == 364
    assert c.is_short_term is False
    assert c.monthly_rent == Decimal("18750.00")


def test_short_term_flag_fires_under_300_days():
    from lib.classes.silver_contract import SilverRentContract

    c = SilverRentContract(
        **_row(
            contract_start_date=date(2026, 9, 20),
            contract_end_date=date(2026, 11, 20),
        )
    )
    assert c.duration_days == 61
    assert c.is_short_term is True


def test_derived_fields_default_none_so_construction_never_fails():
    """A derived field declared without a default is REQUIRED in pydantic v2,
    so mode='after' never runs. This asserts the trap stays closed."""
    from lib.classes.silver_contract import SilverRentContract

    for name in (
        "duration_days",
        "is_short_term",
        "monthly_rent",
        "rent_per_sqft",
        "implied_years",
    ):
        assert name in SilverRentContract.model_fields
        assert not SilverRentContract.model_fields[name].is_required(), name


def test_bad_row_collects_violations_instead_of_raising():
    from lib.classes.silver_contract import SilverRentContract

    c = SilverRentContract(
        **_row(
            annual_amount=Decimal("6650000"),
            actual_area=Decimal("882158"),
            contract_start_date=date(2027, 1, 1),
            contract_end_date=date(2026, 1, 1),
        )
    )
    joined = " | ".join(c.violations)
    assert "annual_amount_above_max" in joined
    assert "actual_area_above_max" in joined
    assert "end_before_start" in joined
    assert c.actual_area == Decimal("882158"), "row is retained, not dropped"


def test_amount_mismatch_is_a_reconciliation_violation():
    """contract_amount/annual_amount == duration in years (corr 0.9956 on the
    real payload). Disagreement >5% means one of the two is wrong."""
    from lib.classes.silver_contract import SilverRentContract

    good = SilverRentContract(**_row())
    assert not [v for v in good.violations if v == "amount_duration_mismatch"]

    bad = SilverRentContract(**_row(contract_amount=Decimal("900000")))
    assert "amount_duration_mismatch" in bad.violations


def test_no_psf_below_200_sqft():
    from lib.classes.silver_contract import SilverRentContract

    assert SilverRentContract(**_row(actual_area=Decimal("199"))).rent_per_sqft is None
    assert SilverRentContract(**_row(actual_area=Decimal("1000"))).psf_eligible is True


def test_rent_per_sqft_lands_in_the_dubai_market_band():
    """Guards the 10.76x m2/sqft corruption, at the point the division happens.
    Spec gate 5: residential flats in the 400-1200 sqft band must land at
    60-120 AED/sqft. Read as m2 the same figures become 868 AED/sqft, roughly
    10x market. Moved here from Task 2, where rent_per_sqft did not yet exist."""
    from lib.classes.silver_contract import SilverRentContract

    c = SilverRentContract(
        **_row(annual_amount=Decimal("60000"), actual_area=Decimal("744"))
    )
    assert c.psf_eligible is True
    assert Decimal("60") <= c.rent_per_sqft <= Decimal("120"), (
        f"rent_per_sqft {c.rent_per_sqft} outside 60-120; the unit is not sqft"
    )
    assert c.rent_per_sqft * Decimal("10.7639") > Decimal("800")


def test_whitespace_is_stripped_under_frozen_config():
    """The strip must go through object.__setattr__. A plain self.area_name_en =
    value raises ValidationError under frozen=True, which is the same silent
    row-loss bug class as the field-bound trap."""
    from lib.classes.silver_contract import SilverRentContract

    c = SilverRentContract(
        **_row(
            area_name_en="  Dubai Marina ",
            ejari_property_type_en="Unit\t",
            ejari_property_sub_type_en=" Flat\n",
        )
    )
    assert c.area_name_en == "Dubai Marina"
    assert c.ejari_property_type_en == "Unit"
    assert c.ejari_property_sub_type_en == "Flat"
