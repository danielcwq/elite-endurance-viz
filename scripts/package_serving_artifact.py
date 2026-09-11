#!/usr/bin/env python3
"""Package the canonical DuckDB snapshot for a read-only web deployment."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT / "data/derived/2024/enduranceviz_2024.duckdb"
DEFAULT_DESTINATION = ROOT / "deploy/enduranceviz_2024.duckdb"
DEFAULT_MANIFEST = ROOT / "deploy/serving-artifact.json"


def sha256_file(source: Path) -> str:
    digest = hashlib.sha256()
    with source.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def snapshot_metadata(database: Path) -> dict[str, object]:
    with duckdb.connect(str(database), read_only=True) as connection:
        row = connection.execute(
            """
            SELECT specification_version, completed_at_utc, source_commit, status
            FROM dataset_builds
            ORDER BY completed_at_utc DESC
            LIMIT 1
            """
        ).fetchone()
        if row is None or row[3] != "succeeded":
            raise RuntimeError("Serving database has no successful dataset build record")
        if str(row[0]).startswith('SYNTHETIC-DEMO'):
            raise RuntimeError('Synthetic demo data must not be packaged for production')
        return {
            "specification_version": row[0],
            "dataset_completed_at_utc": row[1].isoformat(),
            "source_commit": row[2],
        }


def package(source: Path, destination: Path, manifest_path: Path) -> dict[str, object]:
    if not source.is_file():
        raise FileNotFoundError(f"Canonical serving database is missing at {source}")

    metadata = snapshot_metadata(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    shutil.copyfile(source, temporary)
    temporary.replace(destination)

    manifest = {
        "artifact": destination.name,
        "bytes": destination.stat().st_size,
        "packaged_at_utc": datetime.now(timezone.utc).isoformat(),
        "sha256": sha256_file(destination),
        **metadata,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--destination", type=Path, default=DEFAULT_DESTINATION)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    arguments = parser.parse_args()
    manifest = package(arguments.source, arguments.destination, arguments.manifest)
    print(
        f"Packaged {manifest['bytes']:,} bytes at {arguments.destination} "
        f"(sha256 {manifest['sha256']})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
