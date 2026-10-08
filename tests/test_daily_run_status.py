"""Tests for the daily run-status helpers in run_etl_pipeline
(`_data_rows`, `_write_run_status`) — the fail-open no-data record, unrelated to the layers build.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

from run_etl_pipeline import _data_rows, _write_run_status

_CSV_COLUMNS = ["RN", "REGISTRATION_DATE", "ANNUAL_AMOUNT", "PROJECT_EN"]


def _write_csv(path: Path, rows: list[dict]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_data_rows_distinguishes_placeholder_from_data(tmp_path: Path):
    assert _data_rows(tmp_path / "missing.csv") == 0

    placeholder = tmp_path / "empty.csv"
    placeholder.touch()  # downloader's touch() on a no-rows window
    assert _data_rows(placeholder) == 0

    header_only = tmp_path / "header.csv"
    _write_csv(header_only, [])
    assert _data_rows(header_only) == 0

    with_data = tmp_path / "data.csv"
    # wide enough to clear the 100-byte placeholder threshold _data_rows uses
    _write_csv(with_data, [{"RN": 1, "PROJECT_EN": "x" * 80}, {"RN": 2, "PROJECT_EN": "x" * 80}])
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
    payload = json.loads((tmp_path / "etl_status.json").read_text())
    assert payload["outcome"] == "no_data"
    assert payload["data_rows"] == 0
    assert payload["data_date"] == "20260914"
    assert "run_utc" in payload
