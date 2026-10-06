"""Weekly pool — 7-day (Mon-Sun) pooling of the daily Bronze CSVs into one enriched frame.

This is the shared front half of the Silver/Gold builds: it rehydrates the week's daily CSVs,
applies the two freshness gates, cross-file dedupes, and enriches. `build_silver_duckdb` consumes
its output; `build_gold_duckdb` reads the Silver tables it produced.

Two guarantees the pool owes every downstream layer, both enforced before enrichment:

  - Cross-file dedup. The daily extract uses a 2-day window, so the same registration can land in
    two consecutive daily files. Pooled verbatim, those rows double-count into every median. Rows
    repeating a `row_hash` first seen in an EARLIER file are dropped; within-file repeats are kept,
    because the Naif-style bulk blocks repeat one (area, amount) legitimately and the bulk flag must
    still see all of them.
  - Freshness gate. A window pooled from too few daily files, or whose newest registration trails
    the window it claims to cover, is stale — see `WEEKLY_FRESHNESS_GATE` in lib/config.py. The
    build raises rather than publish an artifact that looks current and is not.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
import logging

import polars as pl

from lib.classes.silver_contract import _ROW_HASH_FIELDS, _row_hash
from lib.config import RAW_RENTS_CSV_DTYPES, WEEKLY_FRESHNESS_GATE
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


def resolve_window(
    week: str | None = None, from_date: str | None = None, to_date: str | None = None
) -> tuple[str, date, date]:
    """Return (week, start, end) from either an ISO week or an explicit from/to range."""
    if week:
        start, end = week_to_dates(week)
        return week, start, end
    if from_date and to_date:
        start = datetime.strptime(from_date, "%Y%m%d").date()
        end = datetime.strptime(to_date, "%Y%m%d").date()
        iso = start.isocalendar()
        return f"{iso[0]}W{iso[1]:02d}", start, end
    raise ValueError("Need --week 2026W37 or --from/--to YYYYMMDD")


def _window_days(start: date, end: date) -> list[date]:
    """Every date in [start, end] inclusive."""
    days = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def _daily_csv(d: date) -> Path:
    return Path(f"output/rent_contracts_{d.strftime('%Y%m%d')}.csv")


def _is_usable_csv(p: Path) -> bool:
    """A daily file counts only if it is present, non-trivial, and has data rows.

    The `< 100` byte and `<= 1` line checks are the same heuristic the daily
    pipeline uses to decide a window produced no data; a placeholder `touch()`ed
    by the downloader must not be mistaken for a day of registrations.
    """
    if not p.exists() or p.stat().st_size < 100:
        return False
    try:
        with open(p) as f:
            return sum(1 for _ in f) > 1
    except Exception:
        return False


def collect_week_csvs(start: date, end: date, pattern: str = "output/rent_contracts_*.csv") -> list[Path]:
    return [p for p in (_daily_csv(d) for d in _window_days(start, end)) if _is_usable_csv(p)]


def missing_days(start: date, end: date) -> list[date]:
    """Days in [start, end] with no usable daily CSV — the freshness gate's input."""
    return [d for d in _window_days(start, end) if not _is_usable_csv(_daily_csv(d))]


def add_row_hash(df: pl.DataFrame) -> pl.DataFrame:
    """Attach Silver's cross-day fingerprint to every row.

    Reuses `_row_hash` from the Silver contract rather than restating the five
    fields and their normalization: the fingerprint has to be byte-identical to
    the one Silver assigns, or a row Silver would deduplicate could survive here.
    """
    hashes = [
        _row_hash({field: row.get(field) for field in _ROW_HASH_FIELDS})
        for row in df.iter_rows(named=True)
    ]
    return df.with_columns(pl.Series("row_hash", hashes, dtype=pl.Utf8))


def dedupe_pooled(pooled: pl.DataFrame) -> pl.DataFrame:
    """Drop rows repeating a `row_hash` first seen in an EARLIER daily file.

    Cross-file only. A row is kept iff its file is the earliest file its hash
    appears in, so within-file multiplicity is preserved — the bulk blocks
    (Naif 79x1,540,471) repeat one (area, amount) on purpose and the bulk flag
    downstream must still count every member. `_file_idx` is the file's position
    in the requested window, so "earlier" is deterministic regardless of glob
    order. Requires `row_hash` and `_file_idx` columns.
    """
    first_seen = pooled.group_by("row_hash").agg(
        pl.col("_file_idx").min().alias("_first_idx")
    )
    return (
        pooled.join(first_seen, on="row_hash", how="left")
        .filter(pl.col("_file_idx") == pl.col("_first_idx"))
        .drop(["_file_idx", "_first_idx"])
    )


@dataclass
class PooledWeek:
    """The enriched frame plus everything `_meta` needs to describe the build."""

    frame: pl.DataFrame
    week: str
    start: date
    end: date
    pooled_rows: int
    duplicates_removed: int
    files: list[Path] = field(default_factory=list)
    expected_days: int = 0
    missing: list[date] = field(default_factory=list)
    data_through_date: date | None = None


def pool_and_enrich(
    week: str | None = None, from_date: str | None = None, to_date: str | None = None
) -> PooledWeek:
    """Rehydrate, gate, dedupe and enrich the week's daily CSVs.

    Raises FileNotFoundError if no daily CSV is usable, and RuntimeError if either freshness gate
    fails — the caller must not publish a layer it cannot vouch for.
    """
    week, start, end = resolve_window(week, from_date, to_date)

    files = collect_week_csvs(start, end)
    expected_days = len(_window_days(start, end))
    missing = missing_days(start, end)
    if not files:
        raise FileNotFoundError(
            f"No daily CSVs for {start}..{end} — expected output/rent_contracts_YYYYMMDD.csv "
            f"(fail-open: check Releases)"
        )
    if len(missing) > WEEKLY_FRESHNESS_GATE["max_missing_daily_files"]:
        raise RuntimeError(
            f"Weekly freshness gate failed: {len(missing)} of {expected_days} daily files "
            f"missing/empty for {start}..{end} (allowed "
            f"{WEEKLY_FRESHNESS_GATE['max_missing_daily_files']}): "
            f"{[d.isoformat() for d in missing]}. The daily feed has stalled; refusing to "
            f"publish an artifact that claims to cover this window."
        )

    logger.info(f"Weekly {week} {start}..{end} — using {len(files)}/{expected_days} files: {[f.name for f in files]}")

    dfs = []
    for file_idx, f in enumerate(files):
        # Same pinned dtypes as the daily transformer: per-file inference lets a
        # whole-number money column land Int64 in one file and Float64 in the
        # next, and pl.concat(vertical) below refuses that mismatch.
        df = pl.read_csv(
            str(f),
            encoding="utf8-lossy",
            ignore_errors=True,
            null_values=["null", "NULL", ""],
            schema_overrides=RAW_RENTS_CSV_DTYPES,
        )
        # provenance for cross-file dedup: the file's position in the requested
        # window, so "earlier file" does not depend on column order or glob order.
        dfs.append(df.with_columns(pl.lit(file_idx, dtype=pl.Int64).alias("_file_idx")))
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

    # Freshness: the newest registration in the pool must reach the window it
    # claims to cover. A window end the data never reaches means the daily job
    # has been ingesting something other than the requested days.
    data_through = combined.select(
        pl.col("contract_registration_date").str.to_datetime(strict=False).max()
    ).item()
    data_through_date = data_through.date() if data_through is not None else None
    lag_days = None if data_through_date is None else (end - data_through_date).days
    if lag_days is None or lag_days > WEEKLY_FRESHNESS_GATE["max_contract_date_lag_days"]:
        raise RuntimeError(
            f"Weekly freshness gate failed: newest registration is {data_through_date} "
            f"but the window ends {end} (lag {lag_days} days, allowed "
            f"{WEEKLY_FRESHNESS_GATE['max_contract_date_lag_days']}). The daily feed has "
            f"stalled; refusing to publish a stale artifact."
        )

    # Cross-file dedup MUST run before enrichment: the bulk flag counts rows per
    # (area, amount), so duplicates left in would inflate those counts and could
    # flip a group into bulk. Silver's row_hash is the cross-day key; reuse it.
    pooled_rows = combined.height
    combined = dedupe_pooled(add_row_hash(combined))
    duplicates_removed = pooled_rows - combined.height
    if duplicates_removed:
        logger.warning(
            f"Cross-file dedup removed {duplicates_removed} rows repeating an earlier "
            f"daily file's row_hash ({pooled_rows} pooled -> {combined.height})."
        )

    enriched = enrich_rent_contracts(combined)
    return PooledWeek(
        frame=enriched,
        week=week,
        start=start,
        end=end,
        pooled_rows=pooled_rows,
        duplicates_removed=duplicates_removed,
        files=files,
        expected_days=expected_days,
        missing=missing,
        data_through_date=data_through_date,
    )
