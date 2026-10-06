"""Tests for the Silver DuckDB — the normalized dim/fact tables.

Silver is the contract grain: one FctContract row per deduplicated registration, with the repeated
descriptive text pushed into natural-key dimensions and enrichment columns carried on the fact.
"""
from __future__ import annotations

import csv
from pathlib import Path

import duckdb
import pytest

from lib.analysis.build_silver_duckdb import build_silver_duckdb

_CSV_COLUMNS = [
    "RN", "AREA_EN", "PROP_SUB_TYPE_EN", "PROP_TYPE_EN", "USAGE_EN", "NEAREST_METRO_EN",
    "PROJECT_EN", "MASTER_PROJECT_EN", "REGISTRATION_DATE", "START_DATE", "END_DATE",
    "ANNUAL_AMOUNT", "CONTRACT_AMOUNT", "ACTUAL_AREA", "ROOMS", "TOTAL_PROPERTIES",
    "PARKING", "IS_FREE_HOLD",
]


def _row(rn, registration, area="Dubai Marina", metro="Dubai Marina Metro", amount=80000.0):
    return {
        "RN": rn, "AREA_EN": area, "PROP_SUB_TYPE_EN": "Flat", "PROP_TYPE_EN": "Flat",
        "USAGE_EN": "Residential", "NEAREST_METRO_EN": metro, "PROJECT_EN": "Marina Tower",
        "MASTER_PROJECT_EN": "Dubai Marina", "REGISTRATION_DATE": registration,
        "START_DATE": "2026-09-01T00:00:00", "END_DATE": "2027-08-31T00:00:00",
        "ANNUAL_AMOUNT": amount, "CONTRACT_AMOUNT": amount, "ACTUAL_AREA": 1000.0,
        "ROOMS": 2, "TOTAL_PROPERTIES": 1, "PARKING": 1, "IS_FREE_HOLD": 0,
    }


def _write_csv(path: Path, rows: list[dict]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


@pytest.fixture
def silver_db(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "output"
    out.mkdir()
    _write_csv(
        out / "rent_contracts_20260914.csv",
        [
            _row(1, "2026-09-14T00:00:00", area="Dubai Marina", metro="Dubai Marina Metro", amount=90000.0),
            _row(2, "2026-09-14T00:00:00", area="Business Bay", metro="Business Bay Metro", amount=100000.0),
        ],
    )
    _write_csv(
        out / "rent_contracts_20260915.csv",
        [_row(3, "2026-09-15T00:00:00", area="Business Bay", metro=None, amount=110000.0)],
    )
    return Path(build_silver_duckdb(from_date="20260914", to_date="20260915"))


def test_silver_has_dim_fact_and_meta_tables(silver_db: Path):
    con = duckdb.connect(str(silver_db), read_only=True)
    try:
        tables = {
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
    finally:
        con.close()
    assert {"DimArea", "DimPropertyType", "DimMetro", "FctContract", "_meta"} <= tables


def test_fct_grain_is_one_row_per_deduped_registration(silver_db: Path):
    con = duckdb.connect(str(silver_db), read_only=True)
    try:
        facts = con.execute("SELECT count(*) FROM FctContract").fetchone()[0]
        meta = con.execute("SELECT pooled_rows, deduped_rows FROM _meta").fetchone()
    finally:
        con.close()
    assert facts == meta[1]  # no quarantining here: every deduped row reaches the fact
    assert facts == 3


def test_fct_columns_are_lowercase_and_keys_resolve(silver_db: Path):
    con = duckdb.connect(str(silver_db), read_only=True)
    try:
        cols = [c[0] for c in con.execute("SELECT * FROM FctContract LIMIT 0").description]
        assert all(c == c.lower() for c in cols), cols
        assert {"record_id", "rn", "annual_amount", "has_parking", "is_bulk_registration"} <= set(cols)

        # every fact row resolves its area and property-type keys (they are never null here)
        orphan = con.execute(
            "SELECT count(*) FROM FctContract WHERE area_key IS NULL OR property_type_key IS NULL"
        ).fetchone()[0]
        assert orphan == 0

        # the null-metro contract keeps a null key rather than dropping the row
        assert con.execute("SELECT count(*) FROM FctContract WHERE metro_key IS NULL").fetchone()[0] == 1
    finally:
        con.close()


def test_dims_dedupe_natural_keys(silver_db: Path):
    con = duckdb.connect(str(silver_db), read_only=True)
    try:
        assert con.execute("SELECT count(*) FROM DimArea").fetchone()[0] == 2  # Marina, Business Bay
        assert con.execute("SELECT count(*) FROM DimMetro").fetchone()[0] == 2  # two distinct metros
    finally:
        con.close()
