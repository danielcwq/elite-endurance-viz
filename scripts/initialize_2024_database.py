#!/usr/bin/env python3
"""Create an empty canonical 2024 DuckDB database from the versioned DDL."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEMA = ROOT / "schema" / "2024.sql"
DEFAULT_DATABASE = ROOT / "data" / "curated" / "2024" / "enduranceviz_2024.duckdb"


def initialize_database(schema_path: Path, database_path: Path) -> None:
    if database_path.exists():
        raise FileExistsError(f"Refusing to overwrite existing database: {database_path}")
    if not schema_path.is_file():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(schema_path.read_text(encoding="utf-8"))
    except Exception:
        connection.close()
        database_path.unlink(missing_ok=True)
        raise
    else:
        connection.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        initialize_database(args.schema, args.database)
    except (FileExistsError, FileNotFoundError, duckdb.Error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"Created {args.database}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
