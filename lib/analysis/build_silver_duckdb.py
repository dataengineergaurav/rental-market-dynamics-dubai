"""Build the Silver DuckDB — normalized dim/fact tables for one pooled week.

Usage:
  uv run python -m lib.analysis.build_silver_duckdb --week 2026W37
  uv run python -m lib.analysis.build_silver_duckdb --from 20260913 --to 20260917

Output: output/silver_YYYYWww.duckdb
  Tables: DimArea, DimPropertyType, DimMetro, FctContract, _meta
"""
from __future__ import annotations

import argparse
from pathlib import Path
import logging

import duckdb

from lib.analysis.silver_layer import SILVER_TABLE_SQL, meta_sql
from lib.analysis.weekly_pool import pool_and_enrich

logger = logging.getLogger(__name__)


def build_silver_duckdb(
    week: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    output: str | None = None,
) -> str:
    pooled = pool_and_enrich(week=week, from_date=from_date, to_date=to_date)

    out = output or f"output/silver_{pooled.week}.duckdb"
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    # Regenerated artifact: start clean so no stale table survives from a prior build.
    Path(out).unlink(missing_ok=True)

    con = duckdb.connect(out)
    try:
        con.register("enriched_df", pooled.frame.to_arrow())
        for sql in SILVER_TABLE_SQL:
            con.execute(sql)
        con.execute(meta_sql(pooled))
        fact_rows = con.execute("SELECT count(*) FROM FctContract").fetchone()[0]
    finally:
        con.close()

    logger.info(
        f"Wrote {out} — {Path(out).stat().st_size/1024/1024:.2f} MB, "
        f"{fact_rows} contracts, pooled {pooled.pooled_rows} rows "
        f"({pooled.duplicates_removed} cross-file duplicates removed), "
        f"data through {pooled.data_through_date}"
    )
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Silver DuckDB (dim/fact tables)")
    parser.add_argument("--week", help="ISO week like 2026W37 (Mon-Sun)")
    parser.add_argument("--from", dest="from_date", help="YYYYMMDD start")
    parser.add_argument("--to", dest="to_date", help="YYYYMMDD end")
    parser.add_argument("--output", help="output duckdb path")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    build_silver_duckdb(
        week=args.week, from_date=args.from_date, to_date=args.to_date, output=args.output
    )


if __name__ == "__main__":
    main()
