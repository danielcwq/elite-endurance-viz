#!/usr/bin/env python3
"""Atomically populate a local indexed DuckDB serving database from canonical tables."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.initialize_2024_database import initialize_database
from enduranceviz.database import SERVING_OBJECTS_SQL


def git_commit() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=False
    )
    return result.stdout.strip() or "unknown"


def populate(args: argparse.Namespace) -> dict[str, object]:
    contract = yaml.safe_load(args.contract.read_text(encoding="utf-8"))
    source_commit = git_commit()
    build_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"{contract['dataset']['name']}:{contract['dataset']['specification_version']}:{source_commit}",
        )
    )
    completed = datetime.now(timezone.utc).replace(microsecond=0)
    args.database.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.database.with_suffix(args.database.suffix + ".tmp")
    temporary.unlink(missing_ok=True)
    initialize_database(args.schema, temporary)
    connection = duckdb.connect(str(temporary))
    try:
        athletes = pd.read_csv(args.athletes, dtype=str)
        accounts = pd.read_csv(args.accounts, dtype={"athlete_id": str, "external_account_id": str})
        manifest_paths = args.manifest or [
            ROOT / relative for relative in contract["source_scope"]["manifest_files"]
        ]
        manifest = pd.concat(
            [pd.read_csv(path) for path in manifest_paths], ignore_index=True
        )
        quarantine = pd.read_parquet(args.quarantine)
        for name, frame in (("athlete_input", athletes), ("account_input", accounts)):
            connection.register(name, frame)
        connection.execute(
            """
            INSERT INTO dataset_builds VALUES (?, ?, ?, ?, ?, ?, 'succeeded', ?)
            """,
            [
                build_id,
                contract["dataset"]["name"],
                str(contract["dataset"]["specification_version"]),
                source_commit,
                completed,
                completed,
                str(args.database.parent.relative_to(ROOT)),
            ],
        )
        connection.execute(
            """
            INSERT INTO athletes
            SELECT athlete_id::UUID, official_name, official_name_normalized,
                   nullif(display_name, ''), nullif(nationality_code, ''), gender,
                   identity_status, identity_source,
                   created_at_utc::TIMESTAMPTZ, updated_at_utc::TIMESTAMPTZ
            FROM athlete_input
            """
        )
        connection.execute(
            """
            INSERT INTO athlete_external_accounts
            SELECT provider, external_account_id, athlete_id::UUID,
                   nullif(provider_display_name, ''), match_method, match_status,
                   source_file, try_cast(source_row_number AS UBIGINT)
            FROM account_input
            """
        )
        for table in (
            "performances_2024",
            "activities_2024",
            "data_coverage_2024",
            "weekly_training_2024",
            "athlete_summary_2024",
        ):
            path = args.curated / f"{table}.parquet"
            connection.execute(
                f"INSERT INTO {table} BY NAME SELECT * FROM read_parquet(?)", [str(path)]
            )
        manifest_rows = []
        for row in manifest.to_dict("records"):
            if row.get("read_error") not in (None, "") and not pd.isna(row.get("read_error")):
                continue
            manifest_id = hashlib.sha256(
                f"{build_id}|{row['source_file']}|{row['sha256']}".encode("utf-8")
            ).hexdigest()
            manifest_rows.append(
                {
                    "manifest_id": manifest_id,
                    "build_id": build_id,
                    "source_file": row["source_file"],
                    "source_system": row["source_system"],
                    "source_role": row["source_role"],
                    "sha256": row["sha256"],
                    "byte_count": int(row["byte_count"]),
                    "row_count": int(row["row_count"]),
                    "schema_signature": row["schema_signature"],
                    "extracted_at_utc": row["extraction_time_utc"],
                }
            )
        manifest_frame = pd.DataFrame(manifest_rows)
        connection.register("manifest_input", manifest_frame)
        connection.execute("INSERT INTO import_manifest BY NAME SELECT * FROM manifest_input")
        quarantine = quarantine.copy()
        quarantine["build_id"] = build_id
        quarantine["quarantined_at_utc"] = completed
        connection.register("quarantine_input", quarantine)
        connection.execute(
            """
            INSERT INTO quarantined_records
            SELECT quarantine_id, build_id::UUID, entity_type, source_file,
                   source_row_number, source_record_json::JSON, reason_code,
                   reason_detail, quarantined_at_utc
            FROM quarantine_input
            """
        )
        connection.execute(SERVING_OBJECTS_SQL)
        counts = {
            table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "athletes", "athlete_external_accounts", "performances_2024",
                "activities_2024", "weekly_training_2024", "athlete_summary_2024",
                "quarantined_records", "import_manifest",
            )
        }
        connection.execute("CHECKPOINT")
    except Exception:
        connection.close()
        temporary.unlink(missing_ok=True)
        raise
    connection.close()
    os.replace(temporary, args.database)
    report = {
        "build_id": build_id,
        "dataset": contract["dataset"]["name"],
        "specification_version": str(contract["dataset"]["specification_version"]),
        "source_commit": source_commit,
        "completed_at_utc": completed.isoformat().replace("+00:00", "Z"),
        "database": str(args.database.relative_to(ROOT)),
        "counts": counts,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--schema", type=Path, default=ROOT / "schema/2024.sql")
    result.add_argument("--contract", type=Path, default=ROOT / "config/snapshot_2024.yaml")
    result.add_argument("--athletes", type=Path, default=ROOT / "data/reference/athlete_registry_2024.csv")
    result.add_argument("--accounts", type=Path, default=ROOT / "data/reference/athlete_external_accounts_2024.csv")
    result.add_argument(
        "--manifest",
        type=Path,
        action="append",
        help="Source manifest; repeat to supply multiple manifests (defaults to contract)",
    )
    result.add_argument("--curated", type=Path, default=ROOT / "data/curated/2024")
    result.add_argument("--quarantine", type=Path, default=ROOT / "data/quarantine/2024/quarantined_observations_2024.parquet")
    result.add_argument("--database", type=Path, default=ROOT / "data/derived/2024/enduranceviz_2024.duckdb")
    result.add_argument("--report", type=Path, default=ROOT / "data/manifests/serving_build_2024.json")
    return result


def main() -> int:
    try:
        report = populate(parser().parse_args())
    except (OSError, ValueError, KeyError, duckdb.Error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"Built {report['database']} with {report['counts']['activities_2024']} activities")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
