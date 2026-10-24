"""Week-over-week rent movement — a cross-week report over weekly artifacts.

This is an HONEST TWO-POINT MEASUREMENT, not a trend. It compares two already-
gated weekly medians (`gold_area_median`, which applies the bulk / Hotel /
Labor Camps / Virtual Unit exclusions and the n>=10 floor) and reports the
per-area change between exactly two `data_through` dates. It makes NO claim
beyond those two dates: no smoothing, no projection, no hedonic model, and NO
averaging of area movements into a single city-wide number. That is deliberate —
the BI `rental-index-construction` skill forbids trend/YoY claims on thin data,
and a two-point delta of two defensible medians is a measurement that stays
inside that rule.

Shape: a standalone module (like `gold_indexes.py`). Each weekly DuckDB stays
self-contained; this module READS multiple files, it never ATTACHes one into
another.

Usage:
  uv run python -m lib.analysis.wow_movement --from output/rental_analytics_weekly_2026W37.duckdb \
                                             --to   output/rental_analytics_weekly_2026W38.duckdb
  uv run python -m lib.analysis.wow_movement --series w1.duckdb w2.duckdb w3.duckdb
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path

import duckdb
import polars as pl

from lib.config import MARKET_METRICS

logger = logging.getLogger(__name__)

# The publishable floor has one owner: MARKET_METRICS in lib/config.py, the same
# constant gold_area_median and gold_indexes.MIN_AREA_ROWS read. An area must
# clear it in BOTH weeks to be compared — a median that was defensible last week
# but not this week is not a movement, it is a sample that fell below the floor.
MIN_AREA_ROWS = MARKET_METRICS["min_area_sample_size"]

# Near-floor samples jump easily, so a median built on 11 contracts is flagged
# even when it clears the floor. Tunable heuristic, owner's call.
LOW_CONFIDENCE_MAX_N = 15

_WEEK_FROM_FILENAME = re.compile(r"rental_analytics_weekly_(\w+)\.duckdb")

# The compared-movement schema, emitted even when no area qualifies so a caller
# doing pl.concat or a schema check does not break on an empty frame.
_MOVEMENT_SCHEMA = {
    "area_name_en": pl.Utf8,
    "n_from": pl.Int64,
    "n_to": pl.Int64,
    "median_from": pl.Float64,
    "median_to": pl.Float64,
    "delta_aed": pl.Float64,
    "delta_pct": pl.Float64,
    "low_confidence": pl.Boolean,
}

_SET_CHANGE_SCHEMA = {
    "area_name_en": pl.Utf8,
    "n": pl.Int64,
    "change": pl.Utf8,  # "entered" | "left"
}


def _week_label(path: str, meta: dict) -> str:
    """Prefer the artifact's own week label; fall back to the filename."""
    wk = meta.get("week")
    if wk:
        return str(wk)
    m = _WEEK_FROM_FILENAME.search(Path(path).name)
    return m.group(1) if m else Path(path).stem


def load_area_medians(path: str) -> tuple[pl.DataFrame, dict]:
    """Read `gold_area_median` and `_meta` from a weekly DuckDB (read-only).

    Returns the area-median frame and a metadata dict carrying at least `week`
    and `data_through`. Fails closed if the artifact is unreadable — a bad input
    is an error, not an empty result.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"weekly artifact not found: {path}")

    con = duckdb.connect(str(p), read_only=True)
    try:
        df = pl.from_arrow(
            con.sql(
                "SELECT area_name_en, n, median_rent FROM gold_area_median"
            ).to_arrow_table()
        )
        meta_cols = {
            d[0] for d in con.execute("SELECT * FROM _meta LIMIT 0").description
        }
        wanted = [c for c in ("week", "data_through") if c in meta_cols]
        meta: dict = {}
        if wanted:
            row = con.execute(f"SELECT {', '.join(wanted)} FROM _meta").fetchone()
            if row is not None:
                meta = dict(zip(wanted, row))
    finally:
        con.close()

    meta["week"] = _week_label(path, meta)
    return df, meta


def _qualified(df: pl.DataFrame) -> pl.DataFrame:
    """Rows clearing the n>=10 floor. Inputs already carry the comparable-row
    exclusions from gold_area_median, so this is the only extra gate."""
    return df.filter(pl.col("n") >= MIN_AREA_ROWS)


def compute_movement(from_df: pl.DataFrame, to_df: pl.DataFrame) -> pl.DataFrame:
    """Per-area median movement for areas clearing n>=10 in BOTH weeks.

    No cross-area aggregate is produced — a mean-of-medians would weight a
    10-contract area equal to a 300-contract one and reintroduce exactly the
    mean-over-median error the trust skill rejects.
    """
    left = _qualified(from_df).select(
        pl.col("area_name_en"),
        pl.col("n").alias("n_from"),
        pl.col("median_rent").alias("median_from"),
    )
    right = _qualified(to_df).select(
        pl.col("area_name_en"),
        pl.col("n").alias("n_to"),
        pl.col("median_rent").alias("median_to"),
    )
    joined = left.join(right, on="area_name_en", how="inner")
    if joined.height == 0:
        return pl.DataFrame(schema=_MOVEMENT_SCHEMA)

    return (
        joined.with_columns(
            (pl.col("median_to") - pl.col("median_from")).alias("delta_aed"),
            (
                100.0
                * (pl.col("median_to") - pl.col("median_from"))
                / pl.col("median_from")
            ).alias("delta_pct"),
            (
                pl.col("n_from").is_between(MIN_AREA_ROWS, LOW_CONFIDENCE_MAX_N)
                | pl.col("n_to").is_between(MIN_AREA_ROWS, LOW_CONFIDENCE_MAX_N)
            ).alias("low_confidence"),
        )
        .select(list(_MOVEMENT_SCHEMA.keys()))
        .sort("delta_pct", descending=True)
    )


def set_changes(from_df: pl.DataFrame, to_df: pl.DataFrame) -> pl.DataFrame:
    """Areas that entered or left the n>=10 comparable set between the two weeks.

    Makes a disappearance visible rather than silently dropping it from the join
    (covers an area renamed, split, or that simply fell below the floor).
    """
    from_q = _qualified(from_df).select("area_name_en", "n")
    to_q = _qualified(to_df).select("area_name_en", "n")
    from_set = set(from_q["area_name_en"].to_list())
    to_set = set(to_q["area_name_en"].to_list())

    entered = to_q.filter(
        pl.col("area_name_en").is_in(list(to_set - from_set))
    ).with_columns(pl.lit("entered").alias("change"))
    left = from_q.filter(
        pl.col("area_name_en").is_in(list(from_set - to_set))
    ).with_columns(pl.lit("left").alias("change"))
    out = pl.concat([entered, left], how="vertical")
    if out.height == 0:
        return pl.DataFrame(schema=_SET_CHANGE_SCHEMA)
    return out.select(list(_SET_CHANGE_SCHEMA.keys())).sort(["change", "area_name_en"])


def movement_for_series(paths: list[str]) -> list[dict]:
    """Compute movement for each CONSECUTIVE pair in an ordered list of artifacts.

    Serves a longer time series with no rewrite: [w1, w2, w3] -> w1->w2, w2->w3.
    """
    if len(paths) < 2:
        raise ValueError("movement_for_series needs at least two artifact paths")
    results = []
    loaded = [load_area_medians(p) for p in paths]
    for (from_df, from_meta), (to_df, to_meta) in zip(loaded, loaded[1:]):
        results.append(
            {
                "from_meta": from_meta,
                "to_meta": to_meta,
                "movement": compute_movement(from_df, to_df),
                "set_changes": set_changes(from_df, to_df),
            }
        )
    return results


def _header_lines(result: dict) -> list[str]:
    """`#`-comment provenance header citing exactly what was compared."""
    fm, tm = result["from_meta"], result["to_meta"]
    mv, sc = result["movement"], result["set_changes"]
    entered = int((sc["change"] == "entered").sum()) if sc.height else 0
    left = int((sc["change"] == "left").sum()) if sc.height else 0
    return [
        f"# from_week: {fm.get('week')}",
        f"# to_week: {tm.get('week')}",
        f"# from_data_through: {fm.get('data_through')}",
        f"# to_data_through: {tm.get('data_through')}",
        f"# compared_areas: {mv.height}",
        f"# entered: {entered}",
        f"# left: {left}",
        "# note: two-point measurement of two gated weekly medians; not a trend.",
    ]


def write_movement_csv(result: dict, output_dir: str = "output") -> str:
    """Write output/rent_movement_<fromweek>_<toweek>.csv with a header block."""
    fm, tm = result["from_meta"], result["to_meta"]
    out = Path(output_dir)
    out.mkdir(exist_ok=True)
    path = out / f"rent_movement_{fm.get('week')}_{tm.get('week')}.csv"

    header = "\n".join(_header_lines(result)) + "\n"
    body = result["movement"].write_csv()
    path.write_text(header + body)
    logger.info(
        f"Wrote {path} — {result['movement'].height} areas compared "
        f"({fm.get('week')} -> {tm.get('week')})"
    )
    return str(path)


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Week-over-week rent movement.")
    parser.add_argument("--from", dest="from_path", help="earlier weekly DuckDB")
    parser.add_argument("--to", dest="to_path", help="later weekly DuckDB")
    parser.add_argument("--series", nargs="+", help="ordered list of weekly DuckDBs")
    parser.add_argument("--output-dir", default="output")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)

    if args.series:
        paths = args.series
    elif args.from_path and args.to_path:
        paths = [args.from_path, args.to_path]
    else:
        parser.error("provide --from and --to, or --series with >=2 paths")

    for result in movement_for_series(paths):
        write_movement_csv(result, output_dir=args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
