"""Tests for the week-over-week rent movement module.

Pure functions are tested on in-memory polars frames (no DuckDB needed). One
integration test reads the real W37/W38 artifacts when present and skips
otherwise.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from lib.analysis.wow_movement import (
    compute_movement,
    load_area_medians,
    movement_for_series,
    set_changes,
)


def _medians(rows: list[tuple[str, int, float]]) -> pl.DataFrame:
    """Build a gold_area_median-shaped frame: (area_name_en, n, median_rent)."""
    return pl.DataFrame(
        {
            "area_name_en": [r[0] for r in rows],
            "n": [r[1] for r in rows],
            "median_rent": [r[2] for r in rows],
        }
    )


def test_excludes_area_below_floor_in_either_week():
    frm = _medians([("A", 20, 100_000.0), ("B", 9, 50_000.0)])
    to = _medians([("A", 18, 110_000.0), ("B", 30, 55_000.0)])
    mv = compute_movement(frm, to)
    areas = set(mv["area_name_en"].to_list())
    # B is below floor in the FROM week -> excluded even though TO qualifies
    assert areas == {"A"}


def test_delta_math():
    frm = _medians([("A", 20, 100_000.0)])
    to = _medians([("A", 20, 110_000.0)])
    row = compute_movement(frm, to).row(0, named=True)
    assert row["delta_aed"] == pytest.approx(10_000.0)
    assert row["delta_pct"] == pytest.approx(10.0)


def test_low_confidence_band():
    # n_to = 12 is within [10, 15] -> low confidence; the other area is well above
    frm = _medians([("A", 40, 100_000.0), ("C", 50, 200_000.0)])
    to = _medians([("A", 12, 100_000.0), ("C", 60, 210_000.0)])
    mv = compute_movement(frm, to)
    flags = {r["area_name_en"]: r["low_confidence"] for r in mv.iter_rows(named=True)}
    assert flags["A"] is True
    assert flags["C"] is False


def test_no_cross_area_aggregate():
    frm = _medians([("A", 20, 100_000.0), ("C", 20, 200_000.0)])
    to = _medians([("A", 20, 110_000.0), ("C", 20, 190_000.0)])
    mv = compute_movement(frm, to)
    # one row per area, no synthetic "ALL"/"TOTAL" aggregate row
    assert mv.height == 2
    assert "ALL" not in mv["area_name_en"].to_list()
    assert "TOTAL" not in mv["area_name_en"].to_list()


def test_set_changes_entered_and_left():
    # A stays; B leaves (drops below floor); D enters (rises above floor).
    frm = _medians([("A", 20, 100_000.0), ("B", 20, 50_000.0), ("D", 5, 30_000.0)])
    to = _medians([("A", 20, 110_000.0), ("B", 8, 52_000.0), ("D", 40, 31_000.0)])
    sc = set_changes(frm, to)
    changes = {r["area_name_en"]: r["change"] for r in sc.iter_rows(named=True)}
    assert changes == {"B": "left", "D": "entered"}


def test_empty_result_keeps_schema():
    frm = _medians([("A", 9, 100_000.0)])
    to = _medians([("A", 9, 110_000.0)])
    mv = compute_movement(frm, to)
    assert mv.height == 0
    assert mv.columns == [
        "area_name_en",
        "n_from",
        "n_to",
        "median_from",
        "median_to",
        "delta_aed",
        "delta_pct",
        "low_confidence",
    ]


def test_series_needs_two_paths():
    with pytest.raises(ValueError):
        movement_for_series(["only_one.duckdb"])


# --- integration (skip if the real artifacts are absent) ---

_W37 = Path("output/rental_analytics_weekly_2026W37.duckdb")
_W38 = Path("output/rental_analytics_weekly_2026W38.duckdb")


@pytest.mark.skipif(
    not (_W37.exists() and _W38.exists()),
    reason="real weekly artifacts not present",
)
def test_integration_real_artifacts():
    from_df, from_meta = load_area_medians(str(_W37))
    to_df, to_meta = load_area_medians(str(_W38))
    mv = compute_movement(from_df, to_df)
    assert mv.height > 100  # ~118 areas overlap at n>=10
    assert (mv["n_from"] >= 10).all()
    assert (mv["n_to"] >= 10).all()
    assert from_meta.get("week") and to_meta.get("week")
