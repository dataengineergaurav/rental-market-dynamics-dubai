"""Tests for the Gold DuckDB — analytics views over the attached Silver tables.

Gold holds views only, defined against the `silver` catalog alias. They resolve only while the
Silver DB is attached, which is what `layers.connect_gold` exists for.
"""
from __future__ import annotations

import csv
from pathlib import Path

import duckdb
import pytest

from lib.analysis.build_gold_duckdb import build_gold_duckdb
from lib.analysis.build_silver_duckdb import build_silver_duckdb
from lib.analysis.layers import connect_gold

_CSV_COLUMNS = [
    "RN", "AREA_EN", "PROP_SUB_TYPE_EN", "PROP_TYPE_EN", "USAGE_EN", "NEAREST_METRO_EN",
    "PROJECT_EN", "MASTER_PROJECT_EN", "REGISTRATION_DATE", "START_DATE", "END_DATE",
    "ANNUAL_AMOUNT", "CONTRACT_AMOUNT", "ACTUAL_AREA", "ROOMS", "TOTAL_PROPERTIES",
    "PARKING", "IS_FREE_HOLD",
]

_EXPECTED_VIEWS = {
    "gold_area_median",
    "gold_standard_lease",
    "gold_top_metros_daily",
    "AggAreaRentStats",
    "AggMetroPremium",
    "AggMonthlyRegistrations",
    "AggProjectRentStats",
}


def _row(rn, registration, amount=80000.0):
    return {
        "RN": rn, "AREA_EN": "Dubai Marina", "PROP_SUB_TYPE_EN": "Flat", "PROP_TYPE_EN": "Flat",
        "USAGE_EN": "Residential", "NEAREST_METRO_EN": "Dubai Marina Metro",
        "PROJECT_EN": "Marina Tower", "MASTER_PROJECT_EN": "Dubai Marina",
        "REGISTRATION_DATE": registration, "START_DATE": "2026-09-01T00:00:00",
        "END_DATE": "2027-08-31T00:00:00", "ANNUAL_AMOUNT": amount, "CONTRACT_AMOUNT": amount,
        "ACTUAL_AREA": 1000.0, "ROOMS": 2, "TOTAL_PROPERTIES": 1, "PARKING": 1, "IS_FREE_HOLD": 0,
    }


def _write_csv(path: Path, rows: list[dict]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


@pytest.fixture
def layered_dbs(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out = tmp_path / "output"
    out.mkdir()
    _write_csv(out / "rent_contracts_20260914.csv", [_row(1, "2026-09-14T00:00:00", 90000.0), _row(2, "2026-09-14T00:00:00", 110000.0)])
    _write_csv(out / "rent_contracts_20260915.csv", [_row(3, "2026-09-15T00:00:00", 100000.0)])
    silver = Path(build_silver_duckdb(from_date="20260914", to_date="20260915"))
    gold = Path(build_gold_duckdb(silver))
    return silver, gold


def test_gold_contains_exactly_the_expected_views(layered_dbs):
    _, gold = layered_dbs
    con = duckdb.connect(str(gold), read_only=True)
    try:
        views = {
            r[0]
            for r in con.execute(
                "SELECT view_name FROM duckdb_views() WHERE NOT internal AND schema_name = 'main'"
            ).fetchall()
        }
        tables = {
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
            ).fetchall()
        }
    finally:
        con.close()
    assert views == _EXPECTED_VIEWS
    assert tables == set(), "Gold is views only — no materialized tables"


def test_gold_views_are_unresolvable_without_silver(layered_dbs):
    _, gold = layered_dbs
    con = duckdb.connect(str(gold), read_only=True)
    try:
        with pytest.raises(duckdb.Error):
            con.execute("SELECT * FROM gold_area_median").fetchall()
    finally:
        con.close()


def test_connect_gold_resolves_every_view_and_aggregates_carry_n(layered_dbs):
    _, gold = layered_dbs
    con = connect_gold(gold)
    try:
        for view in _EXPECTED_VIEWS:
            con.execute(f"SELECT count(*) FROM {view}").fetchone()  # must not raise
        for agg in ["AggAreaRentStats", "AggMetroPremium", "AggMonthlyRegistrations", "AggProjectRentStats"]:
            cols = [c[0] for c in con.execute(f"SELECT * FROM {agg} LIMIT 0").description]
            assert "n" in cols or "n_contracts" in cols, (agg, cols)
        # gold_area_median is the shipped contract; it must still expose its six columns
        assert [c[0] for c in con.execute("SELECT * FROM gold_area_median LIMIT 0").description] == [
            "area_name_en", "n", "median_rent", "mean_rent", "min_rent", "max_rent",
        ]
    finally:
        con.close()


def test_gold_views_reference_the_silver_catalog(layered_dbs):
    _, gold = layered_dbs
    con = duckdb.connect(str(gold), read_only=True)
    try:
        sql = con.execute(
            "SELECT sql FROM duckdb_views() WHERE view_name = 'gold_area_median'"
        ).fetchone()[0]
    finally:
        con.close()
    assert "silver.main" in sql
