"""Configuration constants for the NEMSIS data import project."""

import os
from pathlib import Path

# DuckDB database file path
DUCKDB_PATH = os.getenv("DUCKDB_PATH", str(Path(__file__).parent / "ems.duckdb"))
