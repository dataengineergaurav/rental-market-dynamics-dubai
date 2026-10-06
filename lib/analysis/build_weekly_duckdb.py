"""Weekly layer build — orchestrates the Silver and Gold DuckDB builds for one pooled week.

Usage:
  uv run python -m lib.analysis.build_weekly_duckdb --week 2026W37
  uv run python -m lib.analysis.build_weekly_duckdb --from 20260913 --to 20260917
  uv run python -m lib.analysis.build_weekly_duckdb --week 2026W37 --layer silver

Outputs:
  output/silver_YYYYWww.duckdb — normalized tables (DimArea, DimPropertyType, DimMetro,
    FctContract, _meta), built by lib.analysis.build_silver_duckdb
  output/gold_YYYYWww.duckdb   — analytics views over the attached Silver tables, built by
    lib.analysis.build_gold_duckdb

The pooling/dedup/freshness helpers live in lib.analysis.weekly_pool and the view DDL in
lib.analysis.gold_layer; they are re-exported here so existing imports keep working.
"""
from __future__ import annotations

import argparse
import logging

from lib.analysis.build_gold_duckdb import build_gold_duckdb
from lib.analysis.build_silver_duckdb import build_silver_duckdb
from lib.analysis.gold_layer import GOLD_TOP_METROS_DAILY_SQL
from lib.analysis.weekly_pool import (
    add_row_hash,
    collect_week_csvs,
    dedupe_pooled,
    missing_days,
    pool_and_enrich,
    resolve_window,
    week_to_dates,
)

__all__ = [
    "build_weekly_duckdb",
    "build_silver_duckdb",
    "build_gold_duckdb",
    "pool_and_enrich",
    "resolve_window",
    "week_to_dates",
    "collect_week_csvs",
    "missing_days",
    "add_row_hash",
    "dedupe_pooled",
    "GOLD_TOP_METROS_DAILY_SQL",
]

logger = logging.getLogger(__name__)


def build_weekly_duckdb(
    week: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    output: str | None = None,
    silver_output: str | None = None,
    gold_output: str | None = None,
    layer: str = "both",
) -> str:
    """Build the requested layer(s) for the week.

    Returns the primary artifact path: the Gold DuckDB for `both`/`gold`, the Silver DuckDB for
    `silver`.
    """
    silver_path: str | None = None
    if layer in ("silver", "both"):
        silver_path = build_silver_duckdb(
            week=week, from_date=from_date, to_date=to_date, output=silver_output
        )
        if layer == "silver":
            return silver_path

    if silver_path is None:
        # --layer gold: reuse an existing Silver DB for the window rather than rebuilding it.
        _week, _start, _end = resolve_window(week, from_date, to_date)
        silver_path = f"output/silver_{_week}.duckdb"

    return build_gold_duckdb(silver_path, output=gold_output or output)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the weekly Silver and/or Gold DuckDB")
    parser.add_argument("--week", help="ISO week like 2026W37 (Mon-Sun)")
    parser.add_argument("--from", dest="from_date", help="YYYYMMDD start")
    parser.add_argument("--to", dest="to_date", help="YYYYMMDD end")
    parser.add_argument("--layer", choices=["silver", "gold", "both"], default="both")
    parser.add_argument("--output", help="output duckdb path (Gold unless --layer silver)")
    parser.add_argument("--silver-output", help="explicit silver duckdb path")
    parser.add_argument("--gold-output", help="explicit gold duckdb path")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    build_weekly_duckdb(
        week=args.week,
        from_date=args.from_date,
        to_date=args.to_date,
        output=args.output,
        silver_output=args.silver_output,
        gold_output=args.gold_output,
        layer=args.layer,
    )


if __name__ == "__main__":
    main()
