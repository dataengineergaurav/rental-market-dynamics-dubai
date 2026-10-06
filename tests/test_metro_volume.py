"""Tests for gold_top_metros_daily + Pydantic metro volume query.

The view lives in the Gold DB and reads the Silver tables, so the fixture builds a minimal
silver/gold pair and the query attaches Silver (default: the sibling file).
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from lib.analysis.gold_layer import GOLD_TOP_METROS_DAILY_SQL
from lib.analysis.metro_volume import (
    TopMetroDailyRow,
    query_top_metros_daily,
)

# metro_key -> name; the fact carries the key, the dimension the label.
_METROS = {1: "Metro A", 2: "Metro B", 3: "Metro C", 4: "Metro X", 5: "Metro Y", 6: "Metro Z"}


def _seed_silver(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        "CREATE TABLE FctContract (rn INTEGER, metro_key INTEGER, contract_registration_date TIMESTAMP)"
    )
    con.execute("CREATE TABLE DimMetro (metro_key INTEGER, nearest_metro_en VARCHAR)")
    # Day 1: A=3, B=2, C=1 (no 3rd-place tie); null metro ignored
    # Day 2: X=4, Y=2, Z=1
    fact_rows = (
        [(i, 1, "2026-09-15 10:00:00") for i in range(3)]
        + [(i, 2, "2026-09-15 11:00:00") for i in range(3, 5)]
        + [(5, 3, "2026-09-15 12:00:00")]
        + [(7, None, "2026-09-15 14:00:00")]
        + [(i, 4, "2026-09-16 10:00:00") for i in range(10, 14)]
        + [(i, 5, "2026-09-16 11:00:00") for i in range(14, 16)]
        + [(16, 6, "2026-09-16 12:00:00")]
    )
    con.executemany("INSERT INTO FctContract VALUES (?, ?, ?)", fact_rows)
    con.executemany("INSERT INTO DimMetro VALUES (?, ?)", list(_METROS.items()))
    con.execute(
        "CREATE TABLE _meta AS SELECT '2026W37' AS week, "
        "'2026-09-15' AS week_start, '2026-09-21' AS week_end, "
        "15 AS pooled_rows, 2 AS daily_files"
    )


@pytest.fixture
def gold_db(tmp_path: Path) -> Path:
    silver_path = tmp_path / "silver_2026W37.duckdb"
    con = duckdb.connect(str(silver_path))
    try:
        _seed_silver(con)
    finally:
        con.close()

    gold_path = tmp_path / "gold_2026W37.duckdb"
    con = duckdb.connect(str(gold_path))
    try:
        con.execute(f"ATTACH '{silver_path}' AS silver")
        con.execute(GOLD_TOP_METROS_DAILY_SQL)
    finally:
        con.close()
    return gold_path


def test_gold_view_top3_per_day_excludes_null(gold_db: Path):
    result = query_top_metros_daily(gold_db)
    assert result.week == "2026W37"
    assert result.db_path == str(gold_db)

    by_day = {}
    for row in result.rows:
        assert isinstance(row, TopMetroDailyRow)
        assert row.contract_rank in {1, 2, 3}
        by_day.setdefault(row.contract_reg_date, []).append(row)

    assert set(by_day) == {date(2026, 9, 15), date(2026, 9, 16)}
    assert len(by_day[date(2026, 9, 15)]) == 3
    assert len(by_day[date(2026, 9, 16)]) == 3

    day15 = {r.nearest_metro: r for r in by_day[date(2026, 9, 15)]}
    assert day15["Metro A"].contract_rank == 1
    assert day15["Metro A"].number_of_rent_contracts == 3
    assert day15["Metro B"].contract_rank == 2
    assert day15["Metro C"].contract_rank == 3
    assert None not in day15 and all(r.nearest_metro for r in result.rows)

    day16 = sorted(by_day[date(2026, 9, 16)], key=lambda r: r.contract_rank)
    assert [r.nearest_metro for r in day16] == ["Metro X", "Metro Y", "Metro Z"]
    assert day16[0].number_of_rent_contracts == 4


def test_query_missing_db_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        query_top_metros_daily(tmp_path / "missing.duckdb")


def test_query_missing_silver_raises(tmp_path: Path):
    """A Gold DB without its Silver sibling cannot resolve its views — fail loudly, not silently."""
    gold_path = tmp_path / "gold_2026W37.duckdb"
    gold_path.touch()
    with pytest.raises(FileNotFoundError, match="Silver DuckDB not found"):
        query_top_metros_daily(gold_path)
