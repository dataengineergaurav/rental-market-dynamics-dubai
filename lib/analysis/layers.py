"""Reading helper for the cumulative layers DuckDB.

The combined store holds the Silver tables and the Gold views in one file, so a reader needs no
`ATTACH` and no alias bookkeeping — a plain connection resolves every view. Kept as a helper so
consumers and tests open it the same (read-only by default) way.
"""
from __future__ import annotations

from pathlib import Path

import duckdb


def connect_layers(
    path: str | Path,
    read_only: bool = True,
) -> duckdb.DuckDBPyConnection:
    """Open the combined layers DuckDB. The caller owns the connection and must close it."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Layers DuckDB not found: {p}")
    return duckdb.connect(str(p), read_only=read_only)
