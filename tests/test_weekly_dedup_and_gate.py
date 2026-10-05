"""Tests for the weekly builder's cross-file dedup and freshness gate.

Two guarantees the gold views depend on:
  - rows repeating a row_hash first seen in an EARLIER daily file are dropped,
    while within-file repeats survive (bulk registrations lean on this);
  - a window pooled from too few / stale daily files raises instead of shipping.
"""
from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import duckdb
import polars as pl
import pytest

from lib.analysis.build_weekly_duckdb import (
    add_row_hash,
    build_weekly_duckdb,
    collect_week_csvs,
    dedupe_pooled,
    missing_days,
)
from lib.classes.silver_contract import _ROW_HASH_FIELDS, _row_hash
from run_etl_pipeline import _data_rows, _write_run_status

DAY1 = "2026-09-14T00:00:00"
DAY2 = "2026-09-15T00:00:00"
DAY3 = "2026-09-16T00:00:00"

_CSV_COLUMNS = [
    "RN",
    "AREA_EN",
    "PROP_SUB_TYPE_EN",
    "PROP_TYPE_EN",
    "USAGE_EN",
    "NEAREST_METRO_EN",
    "REGISTRATION_DATE",
    "START_DATE",
    "END_DATE",
    "ANNUAL_AMOUNT",
    "ACTUAL_AREA",
]


def _row(rn: int, registration: str, amount: float = 80000.0, area: float = 1000.0) -> dict:
    return {
        "RN": rn,
        "AREA_EN": "Dubai Marina",
        "PROP_SUB_TYPE_EN": "Flat",
        "PROP_TYPE_EN": "Flat",
        "USAGE_EN": "Residential",
        "NEAREST_METRO_EN": "Dubai Marina Metro",
        "REGISTRATION_DATE": registration,
        "START_DATE": "2026-09-01T00:00:00",
        "END_DATE": "2027-08-31T00:00:00",
        "ANNUAL_AMOUNT": amount,
        "ACTUAL_AREA": area,
    }


def _write_csv(path: Path, rows: list[dict]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_dedupe_pooled_keeps_earliest_file_and_within_file_multiplicity():
    pooled = pl.DataFrame(
        {
            # "a" repeats WITHIN file 0 (both must survive); "c" spans files 1 and
            # 2 (file 1 wins, both file-2 rows go).
            "row_hash": ["a", "a", "b", "c", "c", "c"],
            "_file_idx": [0, 0, 1, 1, 2, 2],
            "x": [1, 2, 3, 4, 5, 6],
        }
    )
    out = dedupe_pooled(pooled)
    assert out.sort("x")["x"].to_list() == [1, 2, 3, 4]
    assert "_file_idx" not in out.columns
    assert "_first_idx" not in out.columns


def test_dedupe_pooled_is_noop_without_cross_file_repeats():
    pooled = pl.DataFrame({"row_hash": ["a", "b"], "_file_idx": [0, 1], "x": [1, 2]})
    assert dedupe_pooled(pooled)["x"].to_list() == [1, 2]


def test_add_row_hash_reuses_silver_fingerprint():
    frame = pl.DataFrame(
        {
            "area_name_en": ["Dubai Marina", "Dubai Marina", "Dubai Marina"],
            "ejari_property_sub_type_en": ["Flat", "Flat", "Flat"],
            "contract_start_date": [date(2026, 9, 1), date(2026, 9, 1), date(2026, 9, 1)],
            "annual_amount": [100000.0, 100000.0, 200000.0],
            "actual_area": [1000.0, 1000.0, 1000.0],
        }
    )

    out = add_row_hash(frame)
    # row 0 and 1 are the same contract; row 2 differs only by amount.
    assert out["row_hash"][0] == out["row_hash"][1]
    assert out["row_hash"][0] != out["row_hash"][2]
    assert out["row_hash"][0] == _row_hash(
        {field: frame[field][0] for field in _ROW_HASH_FIELDS}
    )


def test_missing_days_ignores_placeholders(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "output"
    out.mkdir()
    _write_csv(out / "rent_contracts_20260914.csv", [_row(1, DAY1)])
    (out / "rent_contracts_20260915.csv").write_text("")  # downloader placeholder

    assert missing_days(date(2026, 9, 14), date(2026, 9, 16)) == [
        date(2026, 9, 15),
        date(2026, 9, 16),
    ]
    collected = collect_week_csvs(date(2026, 9, 14), date(2026, 9, 16))
    assert [p.name for p in collected] == ["rent_contracts_20260914.csv"]


def test_build_weekly_dedupes_cross_file_and_records_meta(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "output"
    out.mkdir()
    # day 1 carries the contract TWICE (within-file repeat must survive); day 2
    # carries it once (cross-file repeat must be dropped).
    _write_csv(out / "rent_contracts_20260914.csv", [_row(1, DAY1), _row(2, DAY1)])
    _write_csv(out / "rent_contracts_20260915.csv", [_row(3, DAY2)])

    db_path = build_weekly_duckdb(from_date="20260914", to_date="20260915")

    con = duckdb.connect(db_path, read_only=True)
    try:
        meta = con.execute(
            "SELECT pooled_rows, deduped_rows, row_hash_duplicates_removed, "
            "daily_files, expected_daily_files, missing_daily_files, data_through "
            "FROM _meta"
        ).fetchone()
    finally:
        con.close()

    assert meta == (3, 2, 1, 2, 2, 0, "2026-09-15")


def test_data_rows_distinguishes_placeholder_from_data(tmp_path: Path):
    assert _data_rows(tmp_path / "missing.csv") == 0

    placeholder = tmp_path / "empty.csv"
    placeholder.touch()  # downloader's touch() on a no-rows window
    assert _data_rows(placeholder) == 0

    header_only = tmp_path / "header.csv"
    _write_csv(header_only, [])
    assert _data_rows(header_only) == 0

    with_data = tmp_path / "data.csv"
    _write_csv(with_data, [_row(1, DAY1), _row(2, DAY1)])
    assert _data_rows(with_data) == 2


def test_write_run_status_records_no_data(tmp_path: Path):
    _write_run_status(
        tmp_path,
        outcome="no_data",
        data_date="20260914",
        from_date="09/13/2026",
        to_date="09/14/2026",
        data_rows=0,
    )
    import json

    payload = json.loads((tmp_path / "etl_status.json").read_text())
    assert payload["outcome"] == "no_data"
    assert payload["data_rows"] == 0
    assert payload["data_date"] == "20260914"
    assert "run_utc" in payload


def test_build_weekly_gate_rejects_missing_day(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "output"
    out.mkdir()
    _write_csv(out / "rent_contracts_20260914.csv", [_row(1, DAY1)])
    _write_csv(out / "rent_contracts_20260916.csv", [_row(2, DAY3)])

    with pytest.raises(RuntimeError, match="missing/empty"):
        build_weekly_duckdb(from_date="20260914", to_date="20260916")


def test_build_weekly_gate_rejects_stale_window(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "output"
    out.mkdir()
    # Every day present, but the registrations never reach the window end.
    for day in ("20260914", "20260915", "20260916"):
        _write_csv(out / f"rent_contracts_{day}.csv", [_row(1, DAY1)])

    with pytest.raises(RuntimeError, match="freshness gate failed"):
        build_weekly_duckdb(from_date="20260914", to_date="20260916")
