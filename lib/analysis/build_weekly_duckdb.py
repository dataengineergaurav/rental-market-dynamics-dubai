"""
Weekly DuckDB builder — 7-day pooled (Mon-Sun) from Releases daily CSVs.

Usage:
  uv run python -m lib.analysis.build_weekly_duckdb --week 2026W37
  uv run python -m lib.analysis.build_weekly_duckdb --from 20260913 --to 20260917

Output: output/rental_analytics_weekly_2026W37.duckdb
 Tables: fact_rental_contract (enriched, P0-hardened), views: gold_area_median, gold_standard_lease
"""
from __future__ import annotations

import argparse
import calendar
from datetime import date, datetime, timedelta
from pathlib import Path
import logging

import duckdb
import polars as pl

from lib.transform.enrichment import enrich_rent_contracts

logger = logging.getLogger(__name__)


def week_to_dates(week: str) -> tuple[date, date]:
    # 2026W37 -> Mon-Sun
    y = int(week[:4])
    w = int(week[5:])
    # ISO week: Jan 4 is always week 1
    jan4 = date(y, 1, 4)
    start = jan4 + timedelta(weeks=w - 1, days=-jan4.weekday())
    end = start + timedelta(days=6)
    return start, end


def collect_week_csvs(start: date, end: date, pattern: str = "output/rent_contracts_*.csv") -> list[Path]:
    files = []
    cur = start
    while cur <= end:
        name = f"output/rent_contracts_{cur.strftime('%Y%m%d')}.csv"
        p = Path(name)
        if p.exists() and p.stat().st_size >= 100:
            # check has data rows
            try:
                with open(p) as f:
                    if sum(1 for _ in f) > 1:
                        files.append(p)
            except Exception:
                pass
        cur += timedelta(days=1)
    return files


def build_weekly_duckdb(week: str | None = None, from_date: str | None = None, to_date: str | None = None, output: str | None = None) -> str:
    if week:
        start, end = week_to_dates(week)
    elif from_date and to_date:
        start = datetime.strptime(from_date, "%Y%m%d").date()
        end = datetime.strptime(to_date, "%Y%m%d").date()
        # derive week label from start
        iso = start.isocalendar()
        week = f"{iso[0]}W{iso[1]:02d}"
    else:
        raise ValueError("Need --week 2026W37 or --from/--to YYYYMMDD")

    files = collect_week_csvs(start, end)
    if not files:
        raise FileNotFoundError(f"No daily CSVs for {start}..{end} — expected output/rent_contracts_YYYYMMDD.csv (fail-open: check Releases)")

    logger.info(f"Weekly {week} {start}..{end} — using {len(files)} files: {[f.name for f in files]}")

    dfs = [pl.read_csv(str(f), encoding="utf8-lossy", ignore_errors=True, null_values=["null", "NULL", ""]) for f in files]
    combined = pl.concat(dfs, how="vertical")

    aliases = {
        "USAGE_EN": "property_usage_en",
        "ANNUAL_AMOUNT": "annual_amount",
        "ACTUAL_AREA": "actual_area",
        "AREA_EN": "area_name_en",
        "PROJECT_EN": "project_name_en",
        "MASTER_PROJECT_EN": "master_project_en",
        "PROP_TYPE_EN": "ejari_property_type_en",
        "PROP_SUB_TYPE_EN": "ejari_property_sub_type_en",
        "START_DATE": "contract_start_date",
        "END_DATE": "contract_end_date",
        "REGISTRATION_DATE": "contract_registration_date",
        "CONTRACT_AMOUNT": "contract_amount",
        "CONTRACT_NUMBER": "contract_id",
    }
    for src, dst in aliases.items():
        if src in combined.columns and dst not in combined.columns:
            combined = combined.with_columns(pl.col(src).alias(dst))
    combined = combined.with_columns([
        pl.col("annual_amount").cast(pl.Float64, strict=False),
        pl.col("actual_area").cast(pl.Float64, strict=False),
        pl.col("contract_start_date").str.to_datetime(strict=False),
        pl.col("contract_end_date").str.to_datetime(strict=False),
    ])

    enriched = enrich_rent_contracts(combined)

    # output path
    out = output or f"output/rental_analytics_weekly_{week}.duckdb"
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    # create duckdb
    con = duckdb.connect(out)
    # fact = enriched as duckdb table (via arrow)
    con.execute("DROP TABLE IF EXISTS fact_rental_contract")
    # pass via pandas/pl arrow: use create via sql from polars via con.register
    con.register("enriched_df", enriched.to_arrow())
    con.execute("CREATE TABLE fact_rental_contract AS SELECT * FROM enriched_df")
    # gold views
    con.execute("""
        CREATE OR REPLACE VIEW gold_area_median AS
        SELECT area_name_en, count(*) AS n,
               median(annual_amount) AS median_rent,
               avg(annual_amount) AS mean_rent,
               min(annual_amount) AS min_rent,
               max(annual_amount) AS max_rent
        FROM fact_rental_contract
        WHERE ejari_property_sub_type_en NOT IN ('Hotel','Labor Camps')
          AND ejari_property_type_en != 'Virtual Unit'
          AND is_bulk_registration = false
          AND annual_amount IS NOT NULL AND annual_amount > 0
        GROUP BY area_name_en
        HAVING count(*) >= 10
        ORDER BY n DESC
    """)
    con.execute("""
        CREATE OR REPLACE VIEW gold_standard_lease AS
        SELECT count(*) AS n,
               median(annual_amount) AS median_rent,
               avg(annual_amount) AS mean_rent
        FROM fact_rental_contract
        WHERE ejari_property_sub_type_en = 'Flat'
          AND contract_duration_days >= 180 AND contract_duration_days <= 365
          AND ejari_property_type_en != 'Virtual Unit'
          AND is_bulk_registration = false
    """)
    # meta
    con.execute(f"CREATE OR REPLACE VIEW _meta AS SELECT '{week}' AS week, '{start}' AS week_start, '{end}' AS week_end, {enriched.height} AS pooled_rows, {len(files)} AS daily_files")
    con.close()
    logger.info(f"Wrote {out} — {Path(out).stat().st_size/1024/1024:.2f} MB, pooled {enriched.height} rows")
    return out


def main():
    parser = argparse.ArgumentParser(description="Build weekly DuckDB")
    parser.add_argument("--week", help="ISO week like 2026W37 (Mon-Sun)")
    parser.add_argument("--from", dest="from_date", help="YYYYMMDD start")
    parser.add_argument("--to", dest="to_date", help="YYYYMMDD end")
    parser.add_argument("--output", help="output duckdb path")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    build_weekly_duckdb(week=args.week, from_date=args.from_date, to_date=args.to_date, output=args.output)


if __name__ == "__main__":
    main()
