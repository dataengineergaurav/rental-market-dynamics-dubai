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


def _frame(n=3, **over):
    import polars as pl

    rows = [_row(**over) for _ in range(n)]
    return pl.DataFrame(rows)


def test_to_silver_returns_one_row_per_registration():
    from lib.classes.silver_contract import to_silver

    res = to_silver(_frame(3))
    assert len(res.frame) == 3
    assert res.frame["area_name_en"].to_list() == ["Dubai Marina"] * 3


def test_to_silver_never_raises_on_garbage():
    import polars as pl
    from lib.classes.silver_contract import to_silver

    bad = pl.DataFrame(
        {
            "area_name_en": ["", "Dubai Marina", None],
            "annual_amount": [-5.0, 70000.0, 0.0],
            "actual_area": [1.0, 900.0, -3.0],
            "contract_start_date": [None] * 3,
            "contract_end_date": [None] * 3,
        }
    )
    res = to_silver(bad)
    # the null area_name_en row cannot construct, so it is quarantined, not
    # discarded. frame and quarantined are disjoint and together cover the input.
    assert len(res.frame) == 2
    assert len(res.quarantined) == 1
    assert len(res.frame) + len(res.quarantined) == bad.height
    assert res.violation_counts


def test_to_silver_nulls_masked_arabic_cells():
    import polars as pl
    from lib.classes.silver_contract import to_silver

    df = _frame(1, nearest_metro_ar="??", area_name_ar="برج خليفة")
    res = to_silver(df)
    assert res.frame["nearest_metro_ar"][0] is None, "masked cell becomes null"
    assert res.frame["area_name_ar"][0] == "برج خليفة", "intact Arabic survives"
    assert res.violation_counts.get("masked_arabic_cell", 0) == 1


def test_unrecoverable_arabic_columns_are_documented_not_repaired():
    """The upstream DLD mask cannot be undone — the original bytes do not exist.
    So these columns are not 'dropped' by a step in to_silver; they are excluded
    structurally, because extra='ignore' plus model_dump() means only model
    fields can reach the frame. What this test really pins is that the two
    module constants agree: a column listed as unrecoverable must also carry a
    dropped_columns reason, or the documentation silently rots."""
    import polars as pl
    from lib.classes.silver_contract import (
        ARABIC_DROPPED,
        SilverRentContract,
        dropped_columns,
        to_silver,
    )

    for col in ARABIC_DROPPED:
        assert col in dropped_columns, f"{col} has no dropped_columns reason"
        assert col not in SilverRentContract.model_fields, (
            f"{col} is declared as a field but documented as unrecoverable"
        )

    df = pl.DataFrame({
        "area_name_en": ["Dubai Marina"],
        "annual_amount": [90000.0],
        "actual_area": [900.0],
        "VERSION_AR": ["?????"],
    })
    res = to_silver(df)
    assert "VERSION_AR" not in res.frame.columns
    assert len(res.frame) + len(res.quarantined) == df.height


def test_no_row_is_ever_discarded():
    """ADR-03 fail-open: every input row lands in exactly one of frame or
    quarantined. A row that cannot construct is quarantined, never dropped."""
    import polars as pl
    from lib.classes.silver_contract import to_silver

    df = pl.DataFrame({
        "area_name_en": ["Dubai Marina", None, "Business Bay"],
        "annual_amount": [90000.0, 90000.0, 90000.0],
        "actual_area": [900.0, 900.0, -1.0],
    })
    res = to_silver(df)
    assert len(res.frame) + len(res.quarantined) == df.height


def test_row_hash_is_stable_across_runs():
    import polars as pl

    from lib.classes.silver_contract import to_silver

    a = to_silver(pl.DataFrame([{**_row(), "RN": 1}])).frame["row_hash"][0]
    b = to_silver(pl.DataFrame([{**_row(), "RN": 999}])).frame["row_hash"][0]
    assert a == b, "row_hash must not depend on RN or row order"


def test_record_id_is_unique_within_a_file():
    from lib.classes.silver_contract import to_silver

    import polars as pl

    rows = []
    for i in range(50):
        r = _row()
        r["RN"] = i
        rows.append(r)
    frame = to_silver(pl.DataFrame(rows)).frame
    assert frame["record_id"].n_unique() == 50
    assert frame["record_id"].n_unique() == frame.height


def test_record_id_differs_when_row_number_differs():
    from lib.classes.silver_contract import to_silver

    import polars as pl

    a = to_silver(pl.DataFrame([{**_row(), "RN": 1}])).frame["record_id"][0]
    b = to_silver(pl.DataFrame([{**_row(), "RN": 2}])).frame["record_id"][0]
    assert a != b


def test_row_hash_normalises_whitespace_before_hashing():
    """row_hash runs before the model strips whitespace, so it must strip too —
    otherwise two rows that normalise to the same contract hash differently and
    cross-day dedup treats them as distinct."""
    import polars as pl
    from lib.classes.silver_contract import to_silver

    df = pl.DataFrame({
        "area_name_en": ["Dubai Marina", "  Dubai Marina  "],
        "ejari_property_sub_type_en": ["Flat", "Flat"],
        "contract_start_date": [date(2026, 9, 20)] * 2,
        "annual_amount": [90000.0, 90000.0],
        "actual_area": [900.0, 900.0],
    })
    res = to_silver(df)
    assert res.frame["row_hash"][0] == res.frame["row_hash"][1]
    # the model normalises both to the same area too
    assert res.frame["area_name_en"].to_list() == ["Dubai Marina", "Dubai Marina"]


def test_row_hash_distinguishes_zero_from_absent():
    """`or ""` would hash a zero-rent row as if annual_amount were missing, so
    a zero-amount row and a null-amount row would collide. Only None means
    absent."""
    from lib.classes.silver_contract import _row_hash

    base = {
        "area_name_en": "Dubai Marina",
        "ejari_property_sub_type_en": "Flat",
        "contract_start_date": "2026-09-20",
        "annual_amount": None,
        "actual_area": 900,
    }
    zero = {**base, "annual_amount": 0}
    assert _row_hash(zero) != _row_hash(base), "0 must not hash as absent"
    # every falsy zero must clear the None bar, in whatever numeric form
    assert _row_hash({**base, "annual_amount": 0.0}) != _row_hash(base)


def test_row_hash_is_agnostic_to_numeric_type():
    """Cross-day dedup breaks if the same contract hashes differently by input
    type. Production supplies Float64; Decimal and int must agree."""
    from lib.classes.silver_contract import _row_hash

    base = {
        "area_name_en": "Dubai Marina",
        "ejari_property_sub_type_en": "Flat",
        "contract_start_date": "2026-09-20",
        "actual_area": 900,
    }
    assert _row_hash({**base, "annual_amount": 225000.0}) == _row_hash(
        {**base, "annual_amount": Decimal("225000")}
    )
    assert _row_hash({**base, "annual_amount": 225000.0}) == _row_hash(
        {**base, "annual_amount": 225000}
    )
    assert _row_hash({**base, "actual_area": 20.03}) == _row_hash(
        {**base, "actual_area": Decimal("20.03")}
    )


def test_frame_carries_the_full_model_schema():
    """The model is the contract, so the frame must carry every declared field
    plus the two derived keys. Catches a field added to the model but dropped
    before the frame."""
    import polars as pl
    from lib.classes.silver_contract import SilverRentContract, to_silver

    df = pl.DataFrame({
        "area_name_en": ["Dubai Marina"],
        "annual_amount": [90000.0],
        "actual_area": [900.0],
    })
    frame = to_silver(df).frame
    assert set(frame.columns) == set(SilverRentContract.model_fields)
    assert frame.width == len(SilverRentContract.model_fields)


def test_hash_part_never_raises_on_bool():
    """bool subclasses int, so without a guard it routes into Decimal(str(True))
    and raises InvalidOperation — outside to_silver's try block, which would
    break the never-raises contract."""
    from lib.classes.silver_contract import _hash_part

    assert _hash_part(True) == "True"
    assert _hash_part(False) == "False"
    assert _hash_part(0) == "0", "int 0 must still be '0', not 'False'"


def test_hash_part_treats_date_and_datetime_as_the_same_day():
    """The model coerces both to Optional[date], so one contract must not get
    two hashes. datetime is a subclass of date, so it must be checked first."""
    from datetime import date as _date, datetime as _datetime

    from lib.classes.silver_contract import _hash_part

    assert _hash_part(_date(2026, 9, 26)) == _hash_part(_datetime(2026, 9, 26))
    assert _hash_part(_date(2026, 9, 26)) == "2026-09-26"
    assert _hash_part(_datetime(2026, 9, 26, 23, 59, 59)) == "2026-09-26"
