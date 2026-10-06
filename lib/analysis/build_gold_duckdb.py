"""Build the Gold DuckDB — analytics views over the attached Silver tables.

Usage:
  uv run python -m lib.analysis.build_gold_duckdb --silver output/silver_2026W37.duckdb

Output: output/gold_YYYYWww.duckdb
  Views: gold_area_median, gold_standard_lease, gold_top_metros_daily,
         AggAreaRentStats, AggMetroPremium, AggMonthlyRegistrations, AggProjectRentStats

The views reference the Silver database through the catalog alias `silver`, so the Gold file is a
set of definitions that only resolves while Silver is attached. `layers.connect_gold` re-establishes
that attachment when reading.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import logging

import duckdb

from lib.analysis.gold_layer import GOLD_VIEW_SQL

logger = logging.getLogger(__name__)

SILVER_ALIAS = "silver"


def build_gold_duckdb(silver_path: str | Path, output: str | None = None) -> str:
    silver = Path(silver_path)
    if not silver.exists():
        raise FileNotFoundError(f"Silver DuckDB not found: {silver}")

    week = silver.stem.replace("silver_", "")
    out = output or f"output/gold_{week}.duckdb"
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    # Regenerated artifact: a stale view must not survive a schema change.
    Path(out).unlink(missing_ok=True)

    con = duckdb.connect(out)
    try:
        con.execute(f"ATTACH '{silver}' AS {SILVER_ALIAS} (READ_ONLY)")
        for sql in GOLD_VIEW_SQL:
            con.execute(sql)
        views = con.execute(
            "SELECT count(*) FROM duckdb_views() WHERE NOT internal AND schema_name = 'main'"
        ).fetchone()[0]
    finally:
        con.close()

    logger.info(f"Wrote {out} — {views} views over {silver}")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Gold DuckDB (views over Silver)")
    parser.add_argument("--silver", required=True, help="path to silver_YYYYWww.duckdb")
    parser.add_argument("--output", help="output duckdb path")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    build_gold_duckdb(silver_path=args.silver, output=args.output)


if __name__ == "__main__":
    main()
