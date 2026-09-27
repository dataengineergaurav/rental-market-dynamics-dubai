"""Gold index builders.

`build_area_median_index` is the tested home for the rule that the `gold_area_median`
view in `build_weekly_duckdb.py` applies inline. The two must not drift: the SQL
string has no test coverage, so the polars function is the source of truth and
the view should delegate to it.

The exclusions come from docs/IMPLEMENTATION_PLAN.md:46.
"""
from __future__ import annotations

import polars as pl

# `is_bulk_registration` in lib/transform/enrichment.py:235 groups on
# (area_name_en, annual_amount) and flags count > 10. Kept identical on purpose:
# a mass-landlord filing repeats one amount in one area, and pooled 20260913-17
# has 184 such groups (3,987 rows, largest 189 rows for Jabal Ali Industrial
# First at AED 7M) plus Naif 79x1,540,471 and Naif 69x1,900,000.
BULK_GROUP_MAX = 10
MIN_AREA_ROWS = 10

# Hotel and Labor Camps are not market comparables. Virtual Unit is a value of
# ejari_property_TYPE_en, never of the sub_type column (356 vs 0 rows pooled),
# so filtering it on the sub_type silently excludes nothing.
EXCLUDED_SUB_TYPES = ["Hotel", "Labor Camps"]
EXCLUDED_PROPERTY_TYPES = ["Virtual Unit"]


def build_area_median_index(df: pl.DataFrame) -> pl.DataFrame:
    """One row per area, median-led, sorted ascending by median_rent.

    Emits only areas with n >= MIN_AREA_ROWS. When nothing survives, the group_by
    still yields the six published columns at height 0, so a caller doing
    pl.concat or a schema check does not break. polars is unpinned in
    pyproject.toml, so tests assert that empty-frame schema rather than trust it.
    """
    # Same grouping as enrichment._add_bulk_flag, so the two cannot disagree.
    counts = df.group_by(["area_name_en", "annual_amount"]).agg(pl.len().alias("_n"))
    clean = (
        df.join(counts, on=["area_name_en", "annual_amount"], how="left")
        .filter(pl.col("_n") <= BULK_GROUP_MAX)
        .filter(~pl.col("ejari_property_sub_type_en").is_in(EXCLUDED_SUB_TYPES))
        .filter(~pl.col("ejari_property_type_en").is_in(EXCLUDED_PROPERTY_TYPES))
        .filter(pl.col("annual_amount").is_not_null() & (pl.col("annual_amount") > 0))
    )
    return (
        clean.group_by("area_name_en")
        .agg(
            pl.len().alias("n"),
            pl.col("annual_amount").median().alias("median_rent"),
            pl.col("annual_amount").mean().alias("mean_rent"),
            pl.col("annual_amount").min().alias("min_rent"),
            pl.col("annual_amount").max().alias("max_rent"),
        )
        .filter(pl.col("n") >= MIN_AREA_ROWS)
        .sort("median_rent")
    )
