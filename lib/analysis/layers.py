"""Reading helpers for the layered DuckDB releases.

Gold views reference the Silver database through the catalog alias `silver`, so a Gold connection
is only usable once Silver is attached under that exact alias. Centralising it here keeps every
consumer (and test) from re-implementing the attach or getting the alias wrong.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

SILVER_ALIAS = "silver"


def silver_sibling(gold_path: str | Path) -> Path:
    """The Silver file that sits next to a `gold_YYYYWww.duckdb` (same dir, same week)."""
    gold = Path(gold_path)
    return gold.with_name(gold.name.replace("gold_", "silver_", 1))


def connect_gold(
    gold_path: str | Path,
    silver_path: str | Path | None = None,
    read_only: bool = True,
) -> duckdb.DuckDBPyConnection:
    """Open a Gold DuckDB with its Silver database attached as `silver`.

    `silver_path` defaults to the sibling `silver_YYYYWww.duckdb`. The caller owns the connection
    and must close it.
    """
    gold = Path(gold_path)
    if not gold.exists():
        raise FileNotFoundError(f"Gold DuckDB not found: {gold}")
    silver = Path(silver_path) if silver_path is not None else silver_sibling(gold)
    if not silver.exists():
        raise FileNotFoundError(
            f"Silver DuckDB not found: {silver}. The Gold views cannot resolve without it."
        )

    con = duckdb.connect(str(gold), read_only=read_only)
    attach_mode = " (READ_ONLY)" if read_only else ""
    con.execute(f"ATTACH '{silver}' AS {SILVER_ALIAS}{attach_mode}")
    return con
