"""Contract for the raw Ejari CSV schema.

The payload is untyped text and every reader infers unless a column is pinned in
`RAW_RENTS_CSV_DTYPES`. A column left unpinned can flip type between files (a column empty through
the 100-row inference window becomes String; a whole-number money column becomes Int64), which has
broken both the daily and weekly builds. These tests pin the two things that keep the hole closed:
the schema covers every payload column, and the overrides actually win over inference.
"""
from __future__ import annotations

import csv

import polars as pl

from lib.config import RAW_RENTS_CSV_COLUMNS, RAW_RENTS_CSV_DTYPES


def test_schema_covers_every_payload_column():
    assert len(RAW_RENTS_CSV_COLUMNS) == 44
    assert set(RAW_RENTS_CSV_DTYPES) == set(RAW_RENTS_CSV_COLUMNS), (
        "every raw payload column must be pinned, or inference can decide its type. "
        f"unpinned: {sorted(set(RAW_RENTS_CSV_COLUMNS) - set(RAW_RENTS_CSV_DTYPES))}"
    )


def _write_all_numeric(path, n_rows: int = 3) -> None:
    """A CSV where every column holds "0" — text columns would infer Int64 if left unpinned."""
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(RAW_RENTS_CSV_COLUMNS)
        for _ in range(n_rows):
            writer.writerow(["0"] * len(RAW_RENTS_CSV_COLUMNS))


def test_overrides_win_over_inference(tmp_path):
    path = tmp_path / "raw.csv"
    _write_all_numeric(path)

    # Without the schema, a numeric-looking text column is typed as an integer.
    inferred = pl.scan_csv(path).collect_schema()
    assert inferred["AREA_EN"] != pl.Utf8, "fixture must be one inference would mis-type"

    pinned = pl.scan_csv(path, schema_overrides=RAW_RENTS_CSV_DTYPES).collect_schema()
    for name, dtype in RAW_RENTS_CSV_DTYPES.items():
        assert pinned[name] == dtype, f"{name}: pinned {dtype} but got {pinned[name]}"


def test_leading_nulls_do_not_change_a_pinned_type(tmp_path):
    """The PARKING hazard: a column empty through the inference window falls back to String.
    Pinning it must make the inference window irrelevant."""
    path = tmp_path / "raw.csv"
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(RAW_RENTS_CSV_COLUMNS)
        empty = ["", "0", "0"] + [""] * (len(RAW_RENTS_CSV_COLUMNS) - 3)
        for _ in range(120):  # past the 100-row infer_schema_length
            writer.writerow(empty)
        writer.writerow(["0"] * len(RAW_RENTS_CSV_COLUMNS))

    pinned = pl.scan_csv(
        path, null_values=["null", "NULL", ""], schema_overrides=RAW_RENTS_CSV_DTYPES
    ).collect_schema()
    assert pinned["PARKING"] == pl.Int64
    assert pinned["AREA_EN"] == pl.Utf8
