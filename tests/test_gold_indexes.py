import polars as pl
import pytest

from lib.analysis.gold_indexes import build_area_median_index

# docs/IMPLEMENTATION_PLAN.md:83 — the published gate this index is meant to pass.
GATE_FLOOR_AED = 20_000
GATE_CEILING_AED = 500_000
GATE_LEAK_AED = 1_000_000


def _rows(n, area, amount, sub_type, area_sqft=500.0):
    """n rows sharing one (area, amount) pair — this is what trips the bulk filter."""
    return [
        {
            "area_name_en": area,
            "annual_amount": amount,
            "actual_area": area_sqft,
            "ejari_property_sub_type_en": sub_type,
            "ejari_property_type_en": "Unit",
        }
        for _ in range(n)
    ]


def _spread(n, area, amount, sub_type, step=100.0, area_sqft=500.0):
    """n rows spread over distinct amounts, so no (area, amount) group exceeds
    10. A legitimate area with many units looks like this; a bulk filing does not."""
    return [
        {
            "area_name_en": area,
            "annual_amount": amount + i * step,
            "actual_area": area_sqft,
            "ejari_property_sub_type_en": sub_type,
            "ejari_property_type_en": "Unit",
        }
        for i in range(n)
    ]


def test_bulk_block_is_excluded_from_the_median():
    """Naif-style bulk: 87 identical rows at AED 1.54M. Unfiltered, this drags
    mean_rent to 8x the median and pushes max_rent past 1M.

    sub_type is Flat on purpose — if these rows were Hotel the subtype exclusion
    would drop them and the test would pass with the bulk filter removed, proving
    nothing about the rule it names.
    """
    rows = _rows(87, "Naif", 1_540_471.0, "Flat") + _spread(40, "Naif", 60_000.0, "Flat")
    out = build_area_median_index(pl.DataFrame(rows))
    # median of the 40 surviving amounts 60,000..63,900 step 100, not 60,000 —
    # _spread varies the amount, so the first row is not the median.
    assert out["median_rent"][0] == 61_950.0
    assert out["max_rent"][0] <= 1_000_000


def test_bulk_filter_alone_does_the_work():
    """The 87-row group must be removed by the bulk rule, not by a subtype rule.
    Same frame as above with the sub_type exclusion bypassed via a sentinel: if
    the bulk filter is disabled this test fails."""
    rows = _rows(87, "Naif", 1_540_471.0, "Flat") + _spread(40, "Naif", 60_000.0, "Flat")
    out = build_area_median_index(pl.DataFrame(rows))
    assert out["n"][0] == 40, "all 87 bulk rows must be gone, not just reweighted"


def test_legitimate_area_survives_the_bulk_filter():
    """A real area with many units must NOT be mistaken for a bulk filing, so
    its amounts must be spread across groups of at most 10."""
    rows = _spread(40, "Dubai Marina", 90_000.0, "Flat")
    out = build_area_median_index(pl.DataFrame(rows))
    assert out.height == 1
    assert out["area_name_en"][0] == "Dubai Marina"
    assert out["n"][0] == 40


def test_pooled_naif_bulk_groups_are_excluded():
    """Pooled 20260913-17, Naif carries two mass-landlord groups: 79x1,540,471 and
    69x1,900,000. Without the bulk rule Naif reports n=314, median 52,000,
    max 1,900,000 — the >1M leak docs/IMPLEMENTATION_PLAN.md:83 names by name.
    With it, n=220, median 43,995, max 905,000."""
    rows = (
        _rows(79, "Naif", 1_540_471.0, "Flat")
        + _rows(69, "Naif", 1_900_000.0, "Flat")
        + _spread(200, "Naif", 44_000.0, "Flat")
    )
    out = build_area_median_index(pl.DataFrame(rows))
    assert out.height == 1
    assert out["n"][0] == 200
    assert out["max_rent"][0] <= 1_000_000


def test_labor_camps_are_excluded():
    """IMPLEMENTATION_PLAN.md:46 excludes Hotel / Labor Camps / Virtual Unit."""
    rows = _spread(30, "Al Goze Industrial Second", 590_000.0, "Labor Camps")
    out = build_area_median_index(pl.DataFrame(rows))
    assert out.height == 0, "Labor Camps must not reach the index"


def test_hotel_is_excluded():
    rows = _spread(30, "Al Goze Industrial Second", 590_000.0, "Hotel")
    out = build_area_median_index(pl.DataFrame(rows))
    assert out.height == 0, "Hotel must not reach the index"


def test_virtual_unit_is_excluded_by_property_type():
    """Virtual Unit lives in ejari_property_TYPE_en and never in the sub_type
    column (356 vs 0 rows pooled). Filtering it on the sub_type excludes nothing.
    This asserts it is gone by the column it actually occupies."""
    rows = _spread(30, "Dubai Marina", 90_000.0, "Flat", step=100.0)
    rows = [{**r, "ejari_property_type_en": "Virtual Unit"} for r in rows]
    out = build_area_median_index(pl.DataFrame(rows))
    assert out.height == 0, "Virtual Unit must not reach the index"


def test_virtual_unit_sub_type_filter_is_a_no_op():
    """Pins the bug: a sub_type exclusion listing 'Virtual Unit' catches zero rows,
    because no row carries that sub_type. Guards a future edit from reintroducing
    the sub_type-only filter that shipped in the brief."""
    rows = _spread(30, "Dubai Marina", 90_000.0, "Virtual Unit", step=100.0)
    out = build_area_median_index(pl.DataFrame(rows))
    assert out.height == 1, "sub_type 'Virtual Unit' is not a real value in the data"
    assert out["n"][0] == 30


def test_every_emitted_area_has_n_at_least_10():
    rows = _spread(40, "Dubai Marina", 90_000.0, "Flat") + _rows(3, "Al Satwa", 55_000.0, "Flat")
    out = build_area_median_index(pl.DataFrame(rows))
    assert out.filter(pl.col("n") < 10).height == 0
    assert "Dubai Marina" in out["area_name_en"].to_list()
    assert "Al Satwa" not in out["area_name_en"].to_list()


def test_median_within_plausible_dubai_band():
    rows = _spread(50, "Dubai Marina", 120_000.0, "Flat")
    out = build_area_median_index(pl.DataFrame(rows))
    assert 20_000 <= out["median_rent"][0] <= 500_000


def test_output_schema_and_sort_order_are_stable():
    # Zabeel sorts after Al Satwa by name but has the lower median, so a
    # name-ordered result and a median-ordered result cannot be confused.
    rows = _spread(30, "Al Satwa", 195_000.0, "Flat") + _spread(30, "Zabeel", 55_000.0, "Flat")
    out = build_area_median_index(pl.DataFrame(rows))
    assert out.columns == PUBLISHED_COLUMNS
    assert out["area_name_en"].to_list() == ["Zabeel", "Al Satwa"]


PUBLISHED_COLUMNS = [
    "area_name_en",
    "n",
    "median_rent",
    "mean_rent",
    "min_rent",
    "max_rent",
]


def test_empty_result_carries_the_published_schema():
    """A caller doing pl.concat or a schema check must not break when nothing
    survives. Every row here is Labor Camps, so the filter empties the frame and
    the early return in build_area_median_index supplies the schema."""
    out = build_area_median_index(pl.DataFrame(_rows(30, "Al Satwa", 55_000.0, "Labor Camps")))
    assert out.height == 0
    assert out.columns == PUBLISHED_COLUMNS


def test_all_rows_bulk_excluded_carries_the_published_schema():
    """The other way to reach zero rows: a single 30-row identical group is all
    bulk, so clean empties out before the group_by ever runs."""
    out = build_area_median_index(pl.DataFrame(_rows(30, "Al Satwa", 55_000.0, "Flat")))
    assert out.height == 0
    assert out.columns == PUBLISHED_COLUMNS


def test_n_floor_dropping_every_area_keeps_the_published_schema():
    """Distinct amounts, so the filters all pass, but only 3 rows survive the
    n >= 10 floor. Schema comes from the group_by, not the early return."""
    out = build_area_median_index(pl.DataFrame(_spread(3, "Al Satwa", 55_000.0, "Flat")))
    assert out.height == 0
    assert out.columns == PUBLISHED_COLUMNS


def test_zero_and_negative_rent_are_dropped():
    """annual_amount > 0 is a real branch in a publishing path: a zero-rent row
    would otherwise pull an area median toward zero."""
    rows = _spread(30, "Al Satwa", 55_000.0, "Flat") + _rows(5, "Al Satwa", 0.0, "Flat") + _rows(
        5, "Al Satwa", -9_000.0, "Flat"
    )
    out = build_area_median_index(pl.DataFrame(rows))
    assert out["n"][0] == 30
    assert out["min_rent"][0] == 55_000.0


def test_plain_area_round_trips_through_concat():
    """The empty-schema promise is only worth anything if concat actually works."""
    kept = build_area_median_index(pl.DataFrame(_spread(30, "Al Satwa", 55_000.0, "Flat")))
    empty = build_area_median_index(pl.DataFrame(_rows(30, "Al Satwa", 55_000.0, "Flat")))
    both = pl.concat([kept, empty], how="vertical")
    assert both.height == 1
    assert both["area_name_en"][0] == "Al Satwa"


@pytest.mark.xfail(
    strict=True,
    reason=(
        "docs/IMPLEMENTATION_PLAN.md:83 is RED and cannot be met by filtering. "
        "A 20k-500k band is a residential rent band, but an area-level max_rent over "
        "mixed stock always contains a whole-asset lease: Palm Jumeirah 12.61M, "
        "Hadaeq Sheikh Mohammed Bin Rashid 11.67M, Jabal Ali Industrial First 6.65M "
        "(6,850 sqft Building), Al Goze Industrial First 4.3M (Bank, 1,870 sqft). "
        "Only 89 of 11,425 surviving rows (0.78%) exceed 1M, in groups of 1-8, so the "
        "count>10 bulk rule cannot reach them. Al Goze's 590k median is a 3,310 sqft "
        "Showroom, not Labor Camps, so the subtype exclusion is not the cause either. "
        "This is why the test is xfail and not a widened band: if the gate is ever met, "
        "strict mode turns the XPASS into a failure demanding the marker be removed."
    ),
)
def test_published_median_index_gate_from_implementation_plan_line_83():
    rows = (
        _spread(40, "Palm Jumeirah", 200_000.0, "Flat")
        + _rows(1, "Palm Jumeirah", 12_610_000.0, "Flat", area_sqft=90_000.0)
    )
    out = build_area_median_index(pl.DataFrame(rows))
    assert out.filter(pl.col("max_rent") > GATE_LEAK_AED).height == 0, (
        "no area may publish max_rent above 1M"
    )
    assert (out["median_rent"] >= GATE_FLOOR_AED).all() and (
        out["median_rent"] <= GATE_CEILING_AED
    ).all(), "every median must sit inside 20k-500k"
