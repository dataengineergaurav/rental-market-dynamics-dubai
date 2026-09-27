"""Gold index builders.

`build_area_median_index` is a PENDING EXTRACTION, not the source of truth. The
committed `gold_area_median` view in `build_weekly_duckdb.py:116-130` already
applies all three exclusions below, correctly, and ships. This module is a tested
reimplementation of a rule that already works, and nothing in production calls
it — wiring it in is a separate change, held back because
`build_weekly_duckdb.py` carries unrelated uncommitted work. Do not describe the
module and the view as "must not drift": they ALREADY differ on sort (this
module sorts by `median_rent`, the view is `ORDER BY n DESC`), and on n's dtype
(DuckDB `count(*)` is BIGINT, so the cast below has no live divergence either).

Where each exclusion genuinely comes from:
- Hotel / Labor Camps, and the n >= 10 floor, are docs/IMPLEMENTATION_PLAN.md:46.
- Virtual Unit is filtered on ejari_property_type_en, CONTRARY to the plan text.
  plan:46 writes `prop_sub_type NOT IN ('Hotel','Labor Camps','Virtual Unit')`,
  but Virtual Unit is a value of the property_TYPE column and never of the
  sub_type column (356 vs 0 rows pooled 20260913-17), so the plan's filter
  silently excludes nothing. build_weekly_duckdb.py:144 already had this right.
- The count > 10 bulk rule is BULK_GROUP_MAX, below.

plan:46 also specifies `actual_area >= 200`, which neither this module nor the
shipped artifact applies; see task-10-report.md. Not applied here on purpose.
"""
from __future__ import annotations

import polars as pl

from lib.config import MARKET_METRICS
from lib.transform.enrichment import BULK_GROUP_MAX

# `is_bulk_registration` in lib/transform/enrichment.py flags a group on
# (area_name_en, annual_amount) when count > BULK_GROUP_MAX. a mass-landlord
# filing repeats one amount in one area, and pooled 20260913-17 has 184 such
# groups (3,987 rows, largest 189 rows for Jabal Ali Industrial First at AED 7M)
# plus Naif 79x1,540,471 and Naif 69x1,900,000. Read from enrichment rather than
# restated: the bound has one owner, per the plan's Global Constraint.
#
# The n >= 10 floor likewise has one owner: MARKET_METRICS in lib/config.py,
# which MarketAnalytics.analyze_by_area already reads.
MIN_AREA_ROWS = MARKET_METRICS["min_area_sample_size"]

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
    # Same grouping as enrichment._add_bulk_flag, and the same bound, which
    # _add_bulk_flag now exports as BULK_GROUP_MAX. The two can only diverge if
    # someone edits one grouping and not the other; the shared constant removes
    # the half of that risk which was pure duplication.
    counts = df.group_by(["area_name_en", "annual_amount"]).agg(pl.len().alias("_n"))
    clean = (
        df.join(counts, on=["area_name_en", "annual_amount"], how="left")
        .filter(pl.col("_n") <= BULK_GROUP_MAX)
        # ~is_in drops NULLs, exactly as SQL NOT IN does: 58 pooled rows have a
        # NULL ejari_property_sub_type_en and are excluded here without being
        # named in EXCLUDED_SUB_TYPES. Matches the shipped rule; do not "fix".
        .filter(~pl.col("ejari_property_sub_type_en").is_in(EXCLUDED_SUB_TYPES))
        .filter(~pl.col("ejari_property_type_en").is_in(EXCLUDED_PROPERTY_TYPES))
        .filter(pl.col("annual_amount").is_not_null() & (pl.col("annual_amount") > 0))
    )
    return (
        clean.group_by("area_name_en")
        .agg(
            # cast is load-bearing: pl.len() yields UInt32 on polars 1.44.2, but
            # the published artifact's n is Int64, and polars is unpinned.
            pl.len().cast(pl.Int64).alias("n"),
            pl.col("annual_amount").median().alias("median_rent"),
            pl.col("annual_amount").mean().alias("mean_rent"),
            pl.col("annual_amount").min().alias("min_rent"),
            pl.col("annual_amount").max().alias("max_rent"),
        )
        .filter(pl.col("n") >= MIN_AREA_ROWS)
        .sort("median_rent")
    )
