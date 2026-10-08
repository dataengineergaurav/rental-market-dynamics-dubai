"""Tests for the cumulative combined layers DuckDB.

The core new guarantee is idempotency: `contract_id` is the primary key, so re-ingesting a day
(same-day re-run, or the next day's 2-day-window overlap) replaces rows instead of duplicating
them. The Silver tables and Gold views live in the same file, so the views resolve with no
`ATTACH`.
"""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import duckdb
import pytest

from lib.analysis.build_layers_duckdb import ingest_layers
from lib.analysis.layers import connect_layers

_CSV_COLUMNS = [
    "RN",
    "CONTRACT_NUMBER",
    "AREA_EN",
    "PROP_SUB_TYPE_EN",
    "PROP_TYPE_EN",
    "USAGE_EN",
    "NEAREST_METRO_EN",
    "PROJECT_EN",
    "MASTER_PROJECT_EN",
    "REGISTRATION_DATE",
    "START_DATE",
    "END_DATE",
    "ANNUAL_AMOUNT",
    "CONTRACT_AMOUNT",
    "ACTUAL_AREA",
    "ROOMS",
    "TOTAL_PROPERTIES",
    "PARKING",
    "IS_FREE_HOLD",
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


def _row(rn, registration, contract, amount=80000.0, area="Dubai Marina", metro="Dubai Marina Metro"):
    return {
        "RN": rn,
        "CONTRACT_NUMBER": contract,
        "AREA_EN": area,
        "PROP_SUB_TYPE_EN": "Flat",
        "PROP_TYPE_EN": "Flat",
        "USAGE_EN": "Residential",
        "NEAREST_METRO_EN": metro,
        "PROJECT_EN": "Marina Tower",
        "MASTER_PROJECT_EN": "Dubai Marina",
        "REGISTRATION_DATE": registration,
        "START_DATE": "2026-09-01T00:00:00",
        "END_DATE": "2027-08-31T00:00:00",
        "ANNUAL_AMOUNT": amount,
        "CONTRACT_AMOUNT": amount,
        "ACTUAL_AREA": 1000.0,
        "ROOMS": 2,
        "TOTAL_PROPERTIES": 1,
        "PARKING": 1,
        "IS_FREE_HOLD": 0,
    }


def _write_csv(path: Path, rows: list[dict]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    return str(tmp_path / "output" / "rents_layers.duckdb")


def _count(db: str) -> int:
    con = duckdb.connect(db, read_only=True)
    try:
        return con.execute("SELECT count(*) FROM FctContract").fetchone()[0]
    finally:
        con.close()


def test_first_ingest_creates_combined_file(tmp_path: Path, db_path: str):
    csv1 = tmp_path / "rent_contracts_20260914.csv"
    _write_csv(csv1, [_row(1, "2026-09-14T00:00:00", "C1"), _row(2, "2026-09-14T00:00:00", "C2")])

    ingest_layers(db_path=db_path, csv_paths=[csv1])

    assert _count(db_path) == 2
    con = duckdb.connect(db_path, read_only=True)
    try:
        tables = {
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        meta = con.execute("SELECT data_from, data_through, total_contracts FROM _meta").fetchone()
    finally:
        con.close()
    assert {"DimArea", "DimPropertyType", "DimMetro", "FctContract", "_meta"} <= tables
    assert meta == (date(2026, 9, 14), date(2026, 9, 14), 2)


def test_reingest_same_csv_is_idempotent(tmp_path: Path, db_path: str):
    csv1 = tmp_path / "rent_contracts_20260914.csv"
    _write_csv(csv1, [_row(1, "2026-09-14T00:00:00", "C1"), _row(2, "2026-09-14T00:00:00", "C2")])

    ingest_layers(db_path=db_path, csv_paths=[csv1])
    ingest_layers(db_path=db_path, csv_paths=[csv1])

    assert _count(db_path) == 2, "re-ingesting the same day must not duplicate rows"


def test_second_day_grows_without_double_counting_overlap(tmp_path: Path, db_path: str):
    csv1 = tmp_path / "rent_contracts_20260914.csv"
    csv2 = tmp_path / "rent_contracts_20260915.csv"
    _write_csv(csv1, [_row(1, "2026-09-14T00:00:00", "C1"), _row(2, "2026-09-14T00:00:00", "C2")])
    # C2 reappears in day 2's 2-day window; C3 is new.
    _write_csv(csv2, [_row(3, "2026-09-15T00:00:00", "C2"), _row(4, "2026-09-15T00:00:00", "C3")])

    ingest_layers(db_path=db_path, csv_paths=[csv1])
    ingest_layers(db_path=db_path, csv_paths=[csv2])

    assert _count(db_path) == 3, "the overlap row C2 must be replaced, not duplicated"


def test_views_resolve_without_attach(tmp_path: Path, db_path: str):
    csv1 = tmp_path / "rent_contracts_20260914.csv"
    _write_csv(csv1, [_row(1, "2026-09-14T00:00:00", "C1")])
    ingest_layers(db_path=db_path, csv_paths=[csv1])

    con = connect_layers(db_path)  # plain connection, no ATTACH
    try:
        for view in _EXPECTED_VIEWS:
            con.execute(f"SELECT count(*) FROM {view}").fetchone()  # must not raise
        for agg in ["AggAreaRentStats", "AggMetroPremium", "AggMonthlyRegistrations", "AggProjectRentStats"]:
            cols = [c[0] for c in con.execute(f"SELECT * FROM {agg} LIMIT 0").description]
            assert "n" in cols or "n_contracts" in cols, (agg, cols)
        assert [c[0] for c in con.execute("SELECT * FROM gold_area_median LIMIT 0").description] == [
            "area_name_en",
            "n",
            "median_rent",
            "mean_rent",
            "min_rent",
            "max_rent",
        ]
    finally:
        con.close()


def test_monthly_trend_spans_ingested_history(tmp_path: Path, db_path: str):
    """AggMonthlyRegistrations is the reason for a cumulative store: over history it returns
    one row per month, not a single row for a 7-day window."""
    csv1 = tmp_path / "rent_contracts_20260930.csv"
    csv2 = tmp_path / "rent_contracts_20261001.csv"
    _write_csv(csv1, [_row(1, "2026-09-30T00:00:00", "C1")])
    _write_csv(csv2, [_row(2, "2026-10-01T00:00:00", "C2")])

    ingest_layers(db_path=db_path, csv_paths=[csv1])
    ingest_layers(db_path=db_path, csv_paths=[csv2])

    con = connect_layers(db_path)
    try:
        months = [r[0] for r in con.execute("SELECT month FROM AggMonthlyRegistrations ORDER BY month").fetchall()]
    finally:
        con.close()
    assert months == ["2026-09", "2026-10"]


def test_stale_increment_raises(tmp_path: Path, db_path: str):
    """A CSV whose filename claims a data date the registrations never reach is a stalled feed."""
    csv1 = tmp_path / "rent_contracts_20260916.csv"
    _write_csv(csv1, [_row(1, "2026-09-14T00:00:00", "C1")])  # 2 days behind its data date

    with pytest.raises(RuntimeError, match="stalled"):
        ingest_layers(db_path=db_path, csv_paths=[csv1])
    assert not Path(db_path).exists(), "nothing is written when the freshness check fails"


def test_empty_placeholder_is_not_ingested(tmp_path: Path, db_path: str):
    placeholder = tmp_path / "rent_contracts_20260914.csv"
    placeholder.touch()  # downloader's no-data placeholder

    with pytest.raises(FileNotFoundError):
        ingest_layers(db_path=db_path, csv_paths=[placeholder])


def test_meta_star_select_is_fetchable(tmp_path: Path, db_path: str):
    """`SELECT * FROM _meta` must materialize cleanly.

    `built_at` used to be DuckDB `now()` (TIMESTAMPTZ), which needs `pytz` to convert to a Python
    value — a dependency this project does not declare. A naive TIMESTAMP avoids that; this guards
    the regression a workflow smoke-check (`SELECT * FROM _meta`) caught on CI.
    """
    csv1 = tmp_path / "rent_contracts_20260914.csv"
    _write_csv(csv1, [_row(1, "2026-09-14T00:00:00", "C1")])
    ingest_layers(db_path=db_path, csv_paths=[csv1])

    con = duckdb.connect(db_path, read_only=True)
    try:
        row = con.execute("SELECT * FROM _meta").fetchone()
    finally:
        con.close()
    assert row is not None
