"""Tests for gold_top_metros_daily + Pydantic metro volume query."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from lib.analysis.build_weekly_duckdb import GOLD_TOP_METROS_DAILY_SQL
from lib.analysis.metro_volume import (
    TopMetroDailyRow,
    query_top_metros_daily,
)


def _seed_fact(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE fact_rental_contract (
            rn INTEGER,
            nearest_metro_en VARCHAR,
            contract_registration_date TIMESTAMP
        )
        """
    )
    # Day 1: A=3, B=2, C=1 (no 3rd-place tie); null metro ignored
    # Day 2: X=4, Y=2, Z=1
    rows = (
        [(i, "Metro A", "2026-09-15 10:00:00") for i in range(3)]
        + [(i, "Metro B", "2026-09-15 11:00:00") for i in range(3, 5)]
        + [(5, "Metro C", "2026-09-15 12:00:00")]
        + [(7, None, "2026-09-15 14:00:00")]
        + [(i, "Metro X", "2026-09-16 10:00:00") for i in range(10, 14)]
        + [(i, "Metro Y", "2026-09-16 11:00:00") for i in range(14, 16)]
        + [(16, "Metro Z", "2026-09-16 12:00:00")]
    )
    con.executemany(
        "INSERT INTO fact_rental_contract VALUES (?, ?, ?)",
        rows,
    )
    con.execute(GOLD_TOP_METROS_DAILY_SQL)
    con.execute(
        "CREATE OR REPLACE VIEW _meta AS SELECT '2026W37' AS week, "
        "'2026-09-15' AS week_start, '2026-09-21' AS week_end, "
        "15 AS pooled_rows, 2 AS daily_files"
    )


@pytest.fixture
def weekly_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "rental_analytics_weekly_2026W37.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        _seed_fact(con)
    finally:
        con.close()
    return db_path


def test_gold_view_top3_per_day_excludes_null(weekly_db: Path):
    result = query_top_metros_daily(weekly_db)
    assert result.week == "2026W37"
    assert result.db_path == str(weekly_db)

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
