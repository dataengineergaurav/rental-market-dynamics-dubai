"""Gold index builders.

`build_area_median_index` is a PENDING EXTRACTION, not the source of truth. The
committed `gold_area_median` view in `gold_layer.py` already
applies all three exclusions below, correctly, and ships. This module is a tested
reimplementation of a rule that already works, and nothing in production calls
it — wiring it in is a separate change. Do not describe the
module and the view as "must not drift": they ALREADY differ on sort (this
module sorts by `median_rent`, the view is `ORDER BY n DESC`), and on n's dtype
(DuckDB `count(*)` is BIGINT, so the cast below has no live divergence either).

Where each exclusion genuinely comes from:
- Hotel / Labor Camps, and the n >= 10 floor, are docs/IMPLEMENTATION_PLAN.md:46.
- Virtual Unit is filtered on ejari_property_type_en, CONTRARY to the plan text.
  plan:46 writes `prop_sub_type NOT IN ('Hotel','Labor Camps','Virtual Unit')`,
  but Virtual Unit is a value of the property_TYPE column and never of the
  sub_type column (356 vs 0 rows pooled 20260913-17), so the plan's filter
  silently excludes nothing. the shipped `gold_layer.py` already had this right.
- The count > 10 bulk rule is BULK_GROUP_MAX, below.

plan:46 also specifies `actual_area >= 200`, which neither this module nor the
shipped artifact applies; see task-10-report.md. Not applied here on purpose.

`build_residential_market_index` is the gate's measurement surface, not a second
published index: `build_area_median_index` stays on all stock because that is
what the shipped artifact and its consumers read. See the GATE_* block below.
"""

from __future__ import annotations

import polars as pl

from lib.config import MARKET_METRICS, RESIDENTIAL_USAGE
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

# Market-health gate. Defined in
# docs/superpowers/specs/2026-09-27-silver-contract-design.md §3.7 and measured on
# the pooled five daily files (16,075 rows -> 11,425 after _comparable_rows ->
# 103 residential areas at n >= MIN_AREA_ROWS):
#
#   residential median_rent  28,000 - 280,000   +40% / +44% inside the band
#   residential p95_rent     50,001 - 900,000   +11% under the ceiling
#
# These supersede the clause in docs/IMPLEMENTATION_PLAN.md (gitignored), which
# asked for all-stock medians in 20k-500k and no all-stock max_rent > 1M. 26 of
# 121 areas failed it and no filter can satisfy it: a residential rent band was
# being applied to the maximum of mixed stock, so one genuine whole-asset lease
# (Palm Jumeirah 12.61M) failed a market-health gate. The band stays; the
# population and the tail statistic change.
GATE_MEDIAN_FLOOR_AED = 20_000
GATE_MEDIAN_CEILING_AED = 500_000
GATE_P95_CEILING_AED = 1_000_000


def _comparable_rows(df: pl.DataFrame) -> pl.DataFrame:
    """Rows allowed to contribute to a market index. Shared by both builders so
    the published all-stock index and the gate's residential one cannot drift on
    what counts as comparable."""
    # Same grouping as enrichment._add_bulk_flag, and the same bound, which
    # _add_bulk_flag now exports as BULK_GROUP_MAX. The two can only diverge if
    # someone edits one grouping and not the other; the shared constant removes
    # the half of that risk which was pure duplication.
    counts = df.group_by(["area_name_en", "annual_amount"]).agg(pl.len().alias("_n"))
    return (
        df.join(counts, on=["area_name_en", "annual_amount"], how="left")
        .filter(pl.col("_n") <= BULK_GROUP_MAX)
        # ~is_in drops NULLs, exactly as SQL NOT IN does: 58 pooled rows have a
        # NULL ejari_property_sub_type_en and are excluded here without being
        # named in EXCLUDED_SUB_TYPES. Matches the shipped rule; do not "fix".
        .filter(~pl.col("ejari_property_sub_type_en").is_in(EXCLUDED_SUB_TYPES))
        .filter(~pl.col("ejari_property_type_en").is_in(EXCLUDED_PROPERTY_TYPES))
        .filter(pl.col("annual_amount").is_not_null() & (pl.col("annual_amount") > 0))
    )


def build_area_median_index(df: pl.DataFrame) -> pl.DataFrame:
    """One row per area, median-led, sorted ascending by median_rent.

    Emits only areas with n >= MIN_AREA_ROWS. When nothing survives, the group_by
    still yields the six published columns at height 0, so a caller doing
    pl.concat or a schema check does not break. polars is unpinned in
    pyproject.toml, so tests assert that empty-frame schema rather than trust it.
    """
    return (
        _comparable_rows(df)
        .group_by("area_name_en")
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


def build_residential_market_index(df: pl.DataFrame) -> pl.DataFrame:
    """The gate's measurement surface: per-area residential medians plus the
    p95 and max the two GATE_* ceilings are read from.

    Residential-only because 20k-500k is a residential rent band, and because the
    areas that broke the old all-stock gate are the mixed industrial ones
    (Al Goze Industrial First, n=19, median 590,000 across Warehouse / Office /
    Showroom). RESIDENTIAL_USAGE is lib/config.py's list, one owner, and matches
    enrichment's `usage_category == "Residential"` on the pooled five daily files
    (9,372 rows either way) without depending on its substring regex.

    Emits the aggregate, not a pass/fail: the gate is a statement about this
    frame, and a builder that encoded its own verdict could not report a
    violation. `max_rent` is emitted so a reader can see the whole-asset leases
    the p95 clause deliberately tolerates. Empty in, empty out at the published
    n >= MIN_AREA_ROWS floor, carrying the same five columns.
    """
    return (
        _comparable_rows(df)
        .filter(pl.col("property_usage_en").is_in(RESIDENTIAL_USAGE))
        .group_by("area_name_en")
        .agg(
            pl.len().cast(pl.Int64).alias("n"),
            pl.col("annual_amount").median().alias("median_rent"),
            pl.col("annual_amount").quantile(0.95).alias("p95_rent"),
            pl.col("annual_amount").max().alias("max_rent"),
        )
        .filter(pl.col("n") >= MIN_AREA_ROWS)
        .sort("median_rent")
    )
