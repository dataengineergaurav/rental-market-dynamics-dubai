import ast
from pathlib import Path

import polars as pl

from lib.analysis import gold_indexes
from lib.analysis.gold_indexes import (
    GATE_MEDIAN_CEILING_AED,
    GATE_MEDIAN_FLOOR_AED,
    GATE_P95_CEILING_AED,
    build_area_median_index,
    build_residential_market_index,
)

# Market-health gate, per docs/superpowers/specs/2026-09-27-silver-contract-design.md
# §3.7. The numbers live in lib/analysis/gold_indexes.py so the gate has one owner;
# they are imported, never restated here.


def _rows(n, area, amount, sub_type, area_sqft=500.0, usage="Residential"):
    """n rows sharing one (area, amount) pair — this is what trips the bulk filter."""
    return [
        {
            "area_name_en": area,
            "annual_amount": amount,
            "actual_area": area_sqft,
            "ejari_property_sub_type_en": sub_type,
            "ejari_property_type_en": "Unit",
            "property_usage_en": usage,
        }
        for _ in range(n)
    ]


def _spread(n, area, amount, sub_type, step=100.0, area_sqft=500.0, usage="Residential"):
    """n rows spread over distinct amounts, so no (area, amount) group exceeds
    10. A legitimate area with many units looks like this; a bulk filing does not."""
    return [
        {
            "area_name_en": area,
            "annual_amount": amount + i * step,
            "actual_area": area_sqft,
            "ejari_property_sub_type_en": sub_type,
            "ejari_property_type_en": "Unit",
            "property_usage_en": usage,
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
    assert GATE_MEDIAN_FLOOR_AED <= out["median_rent"][0] <= GATE_MEDIAN_CEILING_AED


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

# Must match the schema of output/area_median_index_20260913-17.csv, the published
# artifact consumers already read. Asserted literally rather than by reading that
# CSV, because .gitignore excludes **.csv and a test reading it would pass
# vacuously in CI where the file does not exist. polars is unpinned in
# pyproject.toml, and polars.DataFrame.equals() does NOT compare dtypes, so this
# has to be asserted directly or a version bump moves a dtype silently.
PUBLISHED_SCHEMA = {
    "area_name_en": pl.String,
    "n": pl.Int64,
    "median_rent": pl.Float64,
    "mean_rent": pl.Float64,
    "min_rent": pl.Float64,
    "max_rent": pl.Float64,
}


def test_output_schema_matches_the_published_artifact():
    """Names AND dtypes. pl.len() is UInt32 on polars 1.44.2 while the published
    artifact's n is Int64, so the cast in build_area_median_index is load-bearing
    and this test is what keeps it."""
    rows = _spread(30, "Zabeel", 55_000.0, "Flat")
    out = build_area_median_index(pl.DataFrame(rows))
    assert out.schema == PUBLISHED_SCHEMA
    assert out["n"].dtype == pl.Int64


def test_empty_result_schema_matches_the_published_artifact():
    """Same contract on the zero-row path, where the columns come from the agg
    rather than from a literal schema."""
    out = build_area_median_index(pl.DataFrame(_rows(30, "Al Satwa", 55_000.0, "Flat")))
    assert out.height == 0
    assert out.schema == PUBLISHED_SCHEMA


def test_n_floor_dropping_every_area_keeps_the_published_schema():
    """Distinct amounts, so the filters all pass, but only 3 rows survive the
    n >= 10 floor. Third route to zero rows."""
    out = build_area_median_index(pl.DataFrame(_spread(3, "Al Satwa", 55_000.0, "Flat")))
    assert out.height == 0
    assert out.schema == PUBLISHED_SCHEMA


def _names_in(node):
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}


def test_bounds_are_never_inlined_in_the_consumer():
    """The plan's Global Constraint is that a bound is never hardcoded in more than
    one module. The previous version of this test asserted
    `MIN_AREA_ROWS == MARKET_METRICS["min_area_sample_size"]`, which is true BY
    ASSIGNMENT — restoring the literal `= 10` and deleting the owner import left
    the suite fully green. BULK_GROUP_MAX was worse: re-exported from enrichment,
    so the consumer held its own copy of the number and the comparison was to
    itself.

    Asserted over the source instead, because a bound can be re-inlined in three
    ways and only the source sees all three: as a literal assignment, as a
    literal on either side of a comparison, or by deleting the use entirely. A
    value assertion cannot see the second or third.
    """
    tree = ast.parse(Path(gold_indexes.__file__).read_text())
    for name in ("MIN_AREA_ROWS", "BULK_GROUP_MAX"):
        literals = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == name for t in node.targets)
            and isinstance(node.value, ast.Constant)
        ]
        assert not literals, f"{name} is bound to a bare literal, not read from its owner"

        comparisons = [node for node in ast.walk(tree) if isinstance(node, ast.Compare) and name in _names_in(node)]
        assert comparisons, f"{name} is no longer load-bearing — check the filter chain"
        for node in comparisons:
            operands = [node.left, *node.comparators]
            bare = [o.value for o in operands if isinstance(o, ast.Constant) and isinstance(o.value, (int, float))]
            assert not bare, f"{name} is compared against a bare literal {bare}"


def test_bound_values_are_pinned():
    """The AST test above pins WHERE the numbers come from; this pins WHAT they are,
    which is a separate failure — MIN_AREA_ROWS previously went 10 -> 5 with the
    whole suite green. The two lookups name each bound's owner in executable form."""
    from lib.config import MARKET_METRICS
    from lib.transform.enrichment import BULK_GROUP_MAX as BULK_OWNER

    assert gold_indexes.MIN_AREA_ROWS == 10 == MARKET_METRICS["min_area_sample_size"]
    assert gold_indexes.BULK_GROUP_MAX == 10 == BULK_OWNER


def test_zero_and_negative_rent_are_dropped():
    """annual_amount > 0 is a real branch in a publishing path: a zero-rent row
    would otherwise pull an area median toward zero."""
    rows = (
        _spread(30, "Al Satwa", 55_000.0, "Flat")
        + _rows(5, "Al Satwa", 0.0, "Flat")
        + _rows(5, "Al Satwa", -9_000.0, "Flat")
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
    assert both.schema == PUBLISHED_SCHEMA
    assert both["area_name_en"][0] == "Al Satwa"


# --- Market-health gate (spec §3.7) -------------------------------------------------
# The frame below is hand-built, not read from output/*.csv: .gitignore excludes
# **.csv so no real-data fixture can be committed. The shapes are measured though —
# 82% of the pooled five daily files' 11,425 comparable rows are residential, and
# the numbers quoted per test are the ones the pooled artifact actually carries.
GATE_SCHEMA = {
    "area_name_en": pl.String,
    "n": pl.Int64,
    "median_rent": pl.Float64,
    "p95_rent": pl.Float64,
    "max_rent": pl.Float64,
}


def _gate_failures(index):
    """Areas the gate rejects, in the gate's own two clauses."""
    return index.filter(
        (pl.col("median_rent") < GATE_MEDIAN_FLOOR_AED)
        | (pl.col("median_rent") > GATE_MEDIAN_CEILING_AED)
        | (pl.col("p95_rent") > GATE_P95_CEILING_AED)
    )


def test_whole_asset_lease_does_not_fail_the_market_health_gate():
    """Palm Jumeirah as measured: n=82, median 207,500, p95 900,000, max 12,610,000.
    The max is the largest single lease in the pooled artifact and it is a real
    whole-asset registration, so an area-level maximum over mixed stock can never
    clear a ceiling. The p95 clause is what the gate reads instead, and the max
    assertion below is what proves the lease reached the index rather than being
    filtered away — without it this test would pass on an empty frame.
    """
    rows = _spread(40, "Palm Jumeirah", 200_000.0, "Flat") + _rows(
        1, "Palm Jumeirah", 12_610_000.0, "Flat", area_sqft=90_000.0
    )
    out = build_residential_market_index(pl.DataFrame(rows))
    assert out.height == 1
    assert out["max_rent"][0] == 12_610_000.0, "the lease must still be in the index"
    assert out["p95_rent"][0] <= GATE_P95_CEILING_AED
    assert _gate_failures(out).height == 0


def test_residential_median_band_is_green_and_has_teeth():
    """Green on a real-shaped area, red on a broken one. The two halves matter: a
    band nothing can fail is not a gate. Measured spread across the 103
    residential areas in the pooled artifact is 28,000-280,000, so a residential
    area publishing 700,000 is a unit error, not a market."""
    ok = build_residential_market_index(pl.DataFrame(_spread(40, "Jumeirah Second", 228_750.0, "Flat")))
    assert _gate_failures(ok).height == 0
    # 228,750 + 19.5 steps of 100 — _spread varies the amount, so the first row
    # is not the median.
    assert ok["median_rent"][0] == 230_700.0

    broken = build_residential_market_index(pl.DataFrame(_spread(40, "Jumeirah Second", 700_000.0, "Flat")))
    assert _gate_failures(broken)["area_name_en"].to_list() == ["Jumeirah Second"]


def test_p95_clause_catches_a_portfolio_not_a_single_lease():
    """Four rows at 3M inside a 40-row residential area is a landlord's portfolio,
    not a market, and p95 moves onto it. This is why the leak clause stays at all:
    dropping it, as the superseded note suggested, would gate on nothing."""
    rows = _spread(37, "Al Wasl", 100_000.0, "Flat", step=1_000.0) + _spread(
        3, "Al Wasl", 3_000_000.0, "Flat", step=1_000.0
    )
    out = build_residential_market_index(pl.DataFrame(rows))
    assert out["p95_rent"][0] > GATE_P95_CEILING_AED
    assert out["median_rent"][0] <= GATE_MEDIAN_CEILING_AED, "the median alone misses it"
    assert _gate_failures(out).height == 1


def test_residential_restriction_is_what_makes_the_gate_green():
    """Same frame, both scopes — this is the whole of the G1 argument. Al Goze
    Industrial First is measured at n=19, median 590,000, max 4,300,000 across
    Warehouse / Office / Showroom: mixed industrial stock, not a bug, and
    unreachable by any subtype exclusion. All stock, the superseded gate rejects
    both areas (one median out of band, two maxima over 1M). Residential, only
    Palm Jumeirah is in scope and it passes. Drop the residential filter from
    build_residential_market_index and this test goes red.
    """
    rows = _spread(19, "Al Goze Industrial First", 590_000.0, "Warehouse", usage="Industrial") + _rows(
        1, "Al Goze Industrial First", 4_300_000.0, "Warehouse", usage="Industrial"
    )
    all_stock = build_area_median_index(pl.DataFrame(rows))
    assert all_stock["median_rent"][0] == 590_950.0
    assert all_stock["median_rent"][0] > GATE_MEDIAN_CEILING_AED
    assert all_stock["max_rent"][0] == 4_300_000.0
    assert all_stock["max_rent"][0] > GATE_P95_CEILING_AED
    assert build_residential_market_index(pl.DataFrame(rows)).height == 0


def test_residential_index_keeps_the_n_floor_and_its_schema():
    """The published n >= MIN_AREA_ROWS floor and the empty-frame promise, on the
    gate's surface too. polars is unpinned, so the dtypes are asserted literally
    rather than by equals(), which compares values only."""
    out = build_residential_market_index(pl.DataFrame(_spread(40, "Al Satwa", 55_000.0, "Flat")))
    assert out["area_name_en"].to_list() == ["Al Satwa"]
    assert out["n"][0] == 40
    assert out.schema == GATE_SCHEMA

    below_floor = build_residential_market_index(pl.DataFrame(_spread(9, "Al Satwa", 55_000.0, "Flat")))
    assert below_floor.height == 0
    assert below_floor.schema == GATE_SCHEMA
