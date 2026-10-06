"""Daily combined layers DuckDB — upsert the day's contracts into one cumulative file.

This is the single builder for the layers store. It reads the daily bronze CSV(s), enriches
them, and `INSERT OR REPLACE`s them into a cumulative `FctContract` (keyed on `contract_id`),
then rebuilds the natural-key dimensions and the Gold views in the SAME DuckDB file. No
`ATTACH`, no window, no cross-file dedup code: the primary key makes a re-run and the
consecutive-day 2-day-window overlap idempotent.

Usage:
  uv run python -m lib.analysis.build_layers_duckdb --csv output/rent_contracts_20261005.csv
  uv run python -m lib.analysis.build_layers_duckdb --db output/rents_layers.duckdb --csv a.csv b.csv  # backfill

Output: `output/rents_layers.duckdb` (default) — tables DimArea, DimPropertyType, DimMetro,
FctContract, _meta, plus the seven Gold views.
"""
from __future__ import annotations

import argparse
import logging
import re
from datetime import date, datetime
from pathlib import Path

import duckdb
import polars as pl

from lib.analysis.gold_layer import GOLD_VIEW_SQL
from lib.analysis.silver_layer import (
    SILVER_DDL,
    SILVER_REBUILD_DIMS,
    insert_sql,
    meta_sql,
)
from lib.config import RAW_RENTS_CSV_DTYPES
from lib.transform.enrichment import enrich_rent_contracts

logger = logging.getLogger(__name__)

DEFAULT_DB = "output/rents_layers.duckdb"

# Trust check, not the old freshness gate: the newest registration in the increment may trail
# the data date its filename claims, but no further than this. A stalled feed fails here.
MAX_REGISTRATION_LAG_DAYS = 1

# raw CSV name -> canonical name. The values are what the Silver fact and Gold views expect.
# NEAREST_METRO_EN is aliased here (the weekly raw path used to miss it).
_ALIASES = {
    "USAGE_EN": "property_usage_en",
    "ANNUAL_AMOUNT": "annual_amount",
    "ACTUAL_AREA": "actual_area",
    "AREA_EN": "area_name_en",
    "PROJECT_EN": "project_name_en",
    "MASTER_PROJECT_EN": "master_project_en",
    "PROP_TYPE_EN": "ejari_property_type_en",
    "PROP_SUB_TYPE_EN": "ejari_property_sub_type_en",
    "NEAREST_METRO_EN": "nearest_metro_en",
    "START_DATE": "contract_start_date",
    "END_DATE": "contract_end_date",
    "REGISTRATION_DATE": "contract_registration_date",
    "CONTRACT_AMOUNT": "contract_amount",
    "CONTRACT_NUMBER": "contract_id",
}

# Every enriched-frame column the fact insert reads. Any the enrichment did not produce (a date
# that failed to parse, a sparse test fixture) is backfilled as a typed null so the SQL is static.
_SOURCE_COLUMNS = (
    "contract_id",
    "RN",
    "area_name_en",
    "area_tier",
    "ejari_property_type_en",
    "ejari_property_sub_type_en",
    "property_usage_en",
    "usage_category",
    "nearest_metro_en",
    "contract_registration_date",
    "contract_start_date",
    "contract_end_date",
    "contract_year",
    "contract_quarter",
    "contract_month",
    "contract_weekday",
    "contract_season",
    "annual_amount",
    "contract_amount",
    "actual_area",
    "price_per_sqft",
    "contract_duration_days",
    "contract_duration_category",
    "ROOMS",
    "TOTAL_PROPERTIES",
    "PARKING",
    "IS_FREE_HOLD",
    "is_luxury",
    "is_bulk_registration",
    "project_name_en",
    "master_project_en",
)

_INT = pl.Int64
_DT = pl.Datetime
_NULL_DTYPES = {
    "RN": _INT,
    "contract_year": _INT,
    "contract_quarter": _INT,
    "contract_month": _INT,
    "contract_weekday": _INT,
    "contract_duration_days": _INT,
    "ROOMS": _INT,
    "TOTAL_PROPERTIES": _INT,
    "PARKING": _INT,
    "IS_FREE_HOLD": _INT,
    "annual_amount": pl.Float64,
    "contract_amount": pl.Float64,
    "actual_area": pl.Float64,
    "price_per_sqft": pl.Float64,
    "is_luxury": pl.Boolean,
    "is_bulk_registration": pl.Boolean,
    "contract_start_date": _DT,
    "contract_end_date": _DT,
}

_CSV_NAME_RE = re.compile(r"rent_contracts_(\d{8})\.csv$")


def _usable(path: Path) -> bool:
    """A daily file counts only if present, non-trivial, and has data rows. A placeholder
    `touch()`ed by the downloader must not be ingested as an empty day (which would still
    advance `built_at`)."""
    if not path.exists() or path.stat().st_size < 100:
        return False
    with path.open() as f:
        return sum(1 for _ in f) > 1


def _csv_data_date(path: Path) -> date | None:
    m = _CSV_NAME_RE.search(path.name)
    return datetime.strptime(m.group(1), "%Y%m%d").date() if m else None


def _ensure_columns(df: pl.DataFrame) -> pl.DataFrame:
    missing = [
        (c, _NULL_DTYPES.get(c, pl.Utf8)) for c in _SOURCE_COLUMNS if c not in df.columns
    ]
    if missing:
        df = df.with_columns([pl.lit(None, dtype=dt).alias(c) for c, dt in missing])
    return df


def _prepare_increment(csv_paths: list[Path]) -> pl.DataFrame:
    """Read, canonicalise, enrich and key-dedupe the day's CSV(s) into one frame."""
    frames = []
    for p in csv_paths:
        frames.append(
            pl.read_csv(
                str(p),
                encoding="utf8-lossy",
                ignore_errors=True,
                null_values=["null", "NULL", ""],
                # same pinned dtypes as the daily transformer: per-file inference can flip a
                # whole-number money column to Int64 and break the vstack (2026W40)
                schema_overrides=RAW_RENTS_CSV_DTYPES,
            )
        )
    combined = pl.concat(frames, how="vertical") if len(frames) > 1 else frames[0]

    for src, dst in _ALIASES.items():
        if src in combined.columns and dst not in combined.columns:
            combined = combined.with_columns(pl.col(src).alias(dst))

    # The PK: the payload contract number, falling back to the source row number when absent.
    if "contract_id" not in combined.columns:
        combined = combined.with_columns(pl.col("RN").cast(pl.Utf8).alias("contract_id"))
    else:
        combined = combined.with_columns(
            pl.coalesce([pl.col("contract_id"), pl.col("RN").cast(pl.Utf8)]).alias("contract_id")
        )

    casts = []
    if "annual_amount" in combined.columns:
        casts.append(pl.col("annual_amount").cast(pl.Float64, strict=False))
    if "actual_area" in combined.columns:
        casts.append(pl.col("actual_area").cast(pl.Float64, strict=False))
    if "contract_start_date" in combined.columns:
        casts.append(pl.col("contract_start_date").str.to_datetime(strict=False))
    if "contract_end_date" in combined.columns:
        casts.append(pl.col("contract_end_date").str.to_datetime(strict=False))
    if casts:
        combined = combined.with_columns(casts)

    combined = _ensure_columns(combined)
    enriched = enrich_rent_contracts(combined)

    # Increment-local dedup so the single INSERT OR REPLACE cannot see two rows for one key
    # (DuckDB rejects that). Cross-run overlap is handled by the PK itself.
    enriched = enriched.filter(pl.col("contract_id").is_not_null()).unique(
        subset=["contract_id"], keep="last"
    )
    return enriched


def _check_freshness(increment: pl.DataFrame, expected: date | None) -> None:
    if expected is None:
        return
    newest = increment.select(
        pl.col("contract_registration_date").str.to_datetime(strict=False).max()
    ).item()
    if newest is None:
        return
    lag = (expected - newest.date()).days
    if lag > MAX_REGISTRATION_LAG_DAYS:
        raise RuntimeError(
            f"Layers ingest refused: newest registration {newest.date()} trails the data date "
            f"{expected} by {lag} days (allowed {MAX_REGISTRATION_LAG_DAYS}). The daily feed has "
            f"stalled."
        )


def ingest_layers(
    db_path: str | Path = DEFAULT_DB,
    csv_paths: list[str | Path] | None = None,
    expected_date: date | None = None,
) -> str:
    """Upsert the CSV(s) into the cumulative layers DB and return its path. Idempotent."""
    paths = [Path(p) for p in (csv_paths or [])]
    usable = [p for p in paths if _usable(p)]
    if not usable:
        raise FileNotFoundError(
            f"No usable CSV to ingest (looked at {[str(p) for p in paths]}); expected "
            f"rent_contracts_YYYYMMDD.csv with data rows."
        )

    increment = _prepare_increment(usable)
    if expected_date is None:
        expected_date = max((d for d in (_csv_data_date(p) for p in usable) if d), default=None)
    _check_freshness(increment, expected_date)

    dates = sorted(d for d in (_csv_data_date(p) for p in usable) if d)
    ingested_window = f"{dates[0]}..{dates[-1]}" if dates else "unknown"
    if increment.height == 0:
        logger.warning(f"Increment for {ingested_window} is empty after preparation; nothing to upsert")

    db = Path(db_path)
    db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db))
    try:
        for sql in SILVER_DDL:
            con.execute(sql)
        con.register("increment_df", increment.to_arrow())
        con.execute(insert_sql())
        for sql in SILVER_REBUILD_DIMS:
            con.execute(sql)
        for sql in GOLD_VIEW_SQL:
            con.execute(sql)
        con.execute(meta_sql(ingested_window))
        total = con.execute("SELECT count(*) FROM FctContract").fetchone()[0]
    finally:
        con.close()

    logger.info(
        f"Ingested {increment.height} rows ({ingested_window}) into {db} — {total} contracts total"
    )
    return str(db)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest the daily CSV(s) into the cumulative layers DuckDB")
    parser.add_argument("--db", default=DEFAULT_DB, help=f"layers DuckDB path (default {DEFAULT_DB})")
    parser.add_argument("--csv", nargs="+", required=True, help="one or more rent_contracts_YYYYMMDD.csv")
    parser.add_argument("--expected-date", help="YYYYMMDD override for the freshness check")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    expected = datetime.strptime(args.expected_date, "%Y%m%d").date() if args.expected_date else None
    ingest_layers(db_path=args.db, csv_paths=args.csv, expected_date=expected)


if __name__ == "__main__":
    main()
