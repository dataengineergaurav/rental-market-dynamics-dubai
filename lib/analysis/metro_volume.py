"""
Top metros by daily registration volume — query gold_top_metros_daily from the layers DuckDB.

The combined store holds Silver tables and Gold views in one file, so a plain connection
resolves the view (see lib.analysis.layers).

Usage:
  uv run python -m lib.analysis.metro_volume --db output/rents_layers.duckdb
"""

from __future__ import annotations

import argparse
import csv
import logging
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator

from lib.analysis.layers import connect_layers

logger = logging.getLogger(__name__)


class TopMetroDailyRow(BaseModel):
    nearest_metro: str
    contract_reg_date: date
    number_of_rent_contracts: int = Field(ge=1)
    contract_rank: int = Field(ge=1, le=3)

    @field_validator("contract_reg_date", mode="before")
    @classmethod
    def _coerce_date(cls, v):
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            return date.fromisoformat(v[:10])
        return v


class TopMetroDailyResult(BaseModel):
    rows: List[TopMetroDailyRow]
    db_path: str
    data_through: Optional[date] = None


def query_top_metros_daily(db_path: Path | str) -> TopMetroDailyResult:
    """Read gold_top_metros_daily from the layers DuckDB and validate with Pydantic."""
    path = Path(db_path)

    con = connect_layers(path)
    try:
        records = con.execute(
            """
            SELECT nearest_metro, contract_reg_date,
                   number_of_rent_contracts, contract_rank
            FROM gold_top_metros_daily
            ORDER BY contract_reg_date DESC, contract_rank ASC
            """
        ).fetchall()
        data_through = None
        try:
            data_through = con.execute("SELECT data_through FROM _meta").fetchone()[0]
        except Exception:
            pass
    finally:
        con.close()

    rows = [
        TopMetroDailyRow(
            nearest_metro=r[0],
            contract_reg_date=r[1],
            number_of_rent_contracts=int(r[2]),
            contract_rank=int(r[3]),
        )
        for r in records
    ]
    return TopMetroDailyResult(rows=rows, db_path=str(path), data_through=data_through)


def export_csv(result: TopMetroDailyResult, output: Path | str) -> Path:
    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "nearest_metro",
                "contract_reg_date",
                "number_of_rent_contracts",
                "contract_rank",
            ],
        )
        writer.writeheader()
        for row in result.rows:
            writer.writerow(row.model_dump())
    return out


def main():
    parser = argparse.ArgumentParser(description="Query top metros by registration day")
    parser.add_argument(
        "--db",
        default="output/rents_layers.duckdb",
        help="Path to the layers DuckDB (default output/rents_layers.duckdb)",
    )
    parser.add_argument(
        "--output",
        help="Optional CSV path (default: output/top_metros_daily_<stem>.csv)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    result = query_top_metros_daily(args.db)
    out = Path(args.output) if args.output else Path(f"output/top_metros_daily_{Path(args.db).stem}.csv")
    export_csv(result, out)
    logger.info(
        "Wrote %s — %d rows%s",
        out,
        len(result.rows),
        f" (data through {result.data_through})" if result.data_through else "",
    )
    for row in result.rows[:15]:
        print(f"{row.contract_reg_date}  #{row.contract_rank}  {row.number_of_rent_contracts:5d}  {row.nearest_metro}")
    if len(result.rows) > 15:
        print(f"... ({len(result.rows) - 15} more)")


if __name__ == "__main__":
    main()
