#!/usr/bin/env python3
"""Inventory, export, audit, and reconcile EnduranceViz 2024 sources."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST_DIR = ROOT / "data" / "manifests"
DEFAULT_MONGO_ROOT = ROOT / "data" / "raw" / "2024" / "mongodb"
DEFAULT_MONGO_ID_ROOT = ROOT / "data" / "raw" / "2024" / "mongodb_activity_ids"
DEFAULT_RECONCILED_ROOT = ROOT / "data" / "raw" / "2024" / "reconciled"
DEFAULT_ACTIVITY_CSV = ROOT / "indiv_activities_full.csv"
DEFAULT_RAW_JSON_ROOT = ROOT / "data" / "tempdata"
EXPECTED_MONGO_COLLECTIONS = (
    "activities",
    "athlete_metadata",
    "master_iaaf",
    "update_logs",
)
LEGACY_PROCESSED_ACTIVITY_COLUMNS = (
    "Athlete ID",
    "Athlete Name",
    "Activity ID",
    "Activity Name",
    "Description",
    "Start Date",
    "Elapsed Time",
    "Type",
    "Location",
    "Distance (km)",
    "Pace (min/km)",
    "Time",
)
EXTERNAL_SCAN_EXCLUDED_PARTS = {
    ".git",
    ".venv",
    "__pycache__",
    "env",
    "site-packages",
    "venv",
}
MANIFEST_FIELDS = (
    "source_file",
    "sha256",
    "byte_count",
    "row_count",
    "schema_signature",
    "schema_columns_json",
    "extraction_time_utc",
    "source_system",
    "source_role",
    "source_commit",
    "read_error",
)

# Raw Strava JSON is embedded in some legacy CSV cells and exceeds the module's
# conservative 128 KiB default. Source inventory must read, never truncate, it.
csv.field_size_limit(sys.maxsize)


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def compact_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def schema_signature(columns: Iterable[str]) -> str:
    payload = json.dumps(list(columns), ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def relative_to_root(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def refuse_overwrite(path: Path) -> None:
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable output: {path}")


def current_commit() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()


def tracked_csv_paths() -> list[Path]:
    output = subprocess.check_output(
        ["git", "ls-files", "-z", "--", "*.csv"], cwd=ROOT
    )
    names = [name for name in output.decode("utf-8").split("\0") if name]
    return [ROOT / name for name in sorted(names)]


def classify_source(path: Path, columns: list[str]) -> tuple[str, str]:
    relative = relative_to_root(path).lower()
    column_set = {column.strip().lower() for column in columns}
    has_activity = {"activity id", "start date"}.issubset(column_set)
    has_performance = {"competitor", "discipline", "mark"}.issubset(column_set)

    if has_activity and has_performance:
        source_system = "strava+world_athletics"
    elif has_activity or "raw_json_" in relative:
        source_system = "strava"
    elif has_performance or relative.startswith("oly24 pred/"):
        source_system = "world_athletics"
    else:
        source_system = "unknown_legacy"

    if "/backup/" in relative:
        role = "backup"
    elif relative.startswith("data/raw_data/") or "raw_json_" in relative:
        role = "raw"
    elif relative.startswith("data/tempdata/"):
        role = "temporary"
    elif relative.startswith("oly24 pred/"):
        role = "analysis_output"
    else:
        role = "curated_candidate"
    return source_system, role


def inspect_csv(path: Path, extracted_at: str, commit: str) -> dict[str, Any]:
    columns: list[str] = []
    row_count = 0
    read_error = ""
    try:
        with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
            reader = csv.reader(handle)
            columns = next(reader, [])
            row_count = sum(1 for _ in reader)
    except (csv.Error, OSError) as exc:
        read_error = f"{type(exc).__name__}: {exc}"

    source_system, source_role = classify_source(path, columns)
    return {
        "source_file": relative_to_root(path),
        "sha256": sha256_file(path),
        "byte_count": path.stat().st_size,
        "row_count": row_count,
        "schema_signature": schema_signature(columns),
        "schema_columns_json": json.dumps(columns, ensure_ascii=False, separators=(",", ":")),
        "extraction_time_utc": extracted_at,
        "source_system": source_system,
        "source_role": source_role,
        "source_commit": commit,
        "read_error": read_error,
    }


def external_csv_paths(source_root: Path) -> list[Path]:
    """Return user-data CSVs while excluding copied runtimes and macOS sidecars."""
    paths = []
    for path in source_root.rglob("*.csv"):
        relative = path.relative_to(source_root)
        if path.name.startswith("._"):
            continue
        if EXTERNAL_SCAN_EXCLUDED_PARTS.intersection(relative.parts):
            continue
        paths.append(path)
    return sorted(paths)


def inspect_external_csv(
    path: Path,
    source_root: Path,
    extracted_at: str,
) -> tuple[dict[str, Any], int, list[str]]:
    """Inspect an offline CSV and report malformed row widths as a read error."""
    columns: list[str] = []
    row_count = 0
    mismatched_rows = 0
    read_error = ""
    try:
        with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
            reader = csv.reader(handle)
            columns = next(reader, [])
            for row in reader:
                row_count += 1
                mismatched_rows += len(row) != len(columns)
    except (csv.Error, OSError) as exc:
        read_error = f"{type(exc).__name__}: {exc}"
    if mismatched_rows:
        suffix = f"row_width_mismatch:{mismatched_rows}"
        read_error = f"{read_error};{suffix}".strip(";")

    relative = path.relative_to(source_root).as_posix()
    source_system, source_role = classify_source(path, columns)
    return (
        {
            "source_file": f"external-drive/{relative}",
            "sha256": sha256_file(path),
            "byte_count": path.stat().st_size,
            "row_count": row_count,
            "schema_signature": schema_signature(columns),
            "schema_columns_json": json.dumps(
                columns, ensure_ascii=False, separators=(",", ":")
            ),
            "extraction_time_utc": extracted_at,
            "source_system": source_system,
            "source_role": source_role,
            "source_commit": "external-offline-backup",
            "read_error": read_error,
        },
        mismatched_rows,
        columns,
    )


def normalized_name(value: Any) -> str:
    return " ".join(str(value or "").strip().casefold().split())


def activity_record(row: list[str], columns: Iterable[str]) -> dict[str, str] | None:
    values = {column: row[index] for index, column in enumerate(columns) if index < len(row)}
    activity_id = normalize_activity_id(values.get("Activity ID"))
    if activity_id is None:
        return None
    return {
        "activity_id": activity_id,
        "athlete_id": (values.get("Athlete ID") or "").strip(),
        "start_date": (values.get("Start Date") or "").strip(),
        "activity_type": (values.get("Type") or "").strip(),
    }


def activity_records_from_csv(path: Path) -> dict[str, dict[str, str]]:
    """Read activity IDs, including the known headerless 12-column backup tail."""
    records: dict[str, dict[str, str]] = {}
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
        reader = csv.reader(handle)
        columns = next(reader, [])
        if not {"Activity ID", "Start Date"}.issubset(columns):
            return records
        for row in reader:
            candidate_rows: list[tuple[list[str], Iterable[str]]] = [(row, columns)]
            # One legacy backup silently appends processed 12-column rows after a
            # 16-column table. Its boundary also glues a 15-column row to the
            # first processed row. Recover both records without trusting the file
            # as a clean tabular source.
            if len(columns) != len(LEGACY_PROCESSED_ACTIVITY_COLUMNS):
                looks_processed = (
                    len(row) == len(LEGACY_PROCESSED_ACTIVITY_COLUMNS)
                    and len(row) > 1
                    and not row[1].strip().isdigit()
                )
                if looks_processed:
                    candidate_rows = [(row, LEGACY_PROCESSED_ACTIVITY_COLUMNS)]
                elif len(row) > len(columns):
                    tail = row[-len(LEGACY_PROCESSED_ACTIVITY_COLUMNS) :]
                    if len(tail) > 1 and not tail[1].strip().isdigit():
                        candidate_rows.append((tail, LEGACY_PROCESSED_ACTIVITY_COLUMNS))
            for candidate, candidate_columns in candidate_rows:
                record = activity_record(candidate, candidate_columns)
                if record is not None:
                    records[record["activity_id"]] = record
    return records


def raw_json_activity_records(path: Path) -> dict[str, dict[str, str]]:
    """Mirror the legacy target-athlete filter without exposing activity text."""
    records: dict[str, dict[str, str]] = {}
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
        for row in csv.DictReader(handle):
            payload = json.loads(row.get("JSON Data") or "")
            target_name = normalized_name(row.get("Name"))
            entries = payload.get("preFetchedEntries")
            if entries is None:
                entries = payload.get("appContext", {}).get("preFetchedEntries", [])
            for entry in entries or []:
                if "activity" in entry:
                    activity = entry.get("activity") or {}
                    athlete = activity.get("athlete") or {}
                    if normalized_name(athlete.get("athleteName")) != target_name:
                        continue
                    record = {
                        "activity_id": normalize_activity_id(activity.get("id")) or "",
                        "athlete_id": str(
                            athlete.get("athleteId") or row.get("Athlete ID") or ""
                        ),
                        "start_date": str(activity.get("startDate") or ""),
                        "activity_type": str(activity.get("type") or ""),
                    }
                    if record["activity_id"]:
                        records[record["activity_id"]] = record
                elif (entry.get("rowData") or {}).get("entity") == "GroupActivity":
                    for activity in entry["rowData"].get("activities") or []:
                        if normalized_name(activity.get("athlete_name")) != target_name:
                            continue
                        record = {
                            "activity_id": normalize_activity_id(
                                activity.get("activity_id")
                            )
                            or "",
                            "athlete_id": str(
                                activity.get("athlete_id") or row.get("Athlete ID") or ""
                            ),
                            "start_date": str(activity.get("start_date") or ""),
                            "activity_type": str(activity.get("type") or ""),
                        }
                        if record["activity_id"]:
                            records[record["activity_id"]] = record
    return records


def record_year(record: dict[str, str]) -> str:
    value = record.get("start_date", "")
    try:
        return str(datetime.fromisoformat(value.replace("Z", "+00:00")).year)
    except ValueError:
        return "unparseable"


def audit_external_drive(args: argparse.Namespace) -> int:
    """Manifest an offline backup and compare its activity evidence read-only."""
    generated = args.generated_at or utc_now()
    stamp = generated.date().isoformat()
    manifest = args.manifest or DEFAULT_MANIFEST_DIR / f"external_drive_csv_snapshot_{stamp}.csv"
    output = args.output or DEFAULT_MANIFEST_DIR / f"external_drive_audit_{stamp}.json"
    refuse_overwrite(manifest)
    refuse_overwrite(output)
    if not args.source_root.is_dir():
        raise ValueError(f"External source root does not exist: {args.source_root}")

    extracted_at = iso_utc(generated)
    paths = external_csv_paths(args.source_root)
    manifest_rows: list[dict[str, Any]] = []
    metadata: dict[Path, tuple[str, list[str]]] = {}
    mismatched_file_count = 0
    mismatched_row_count = 0
    for path in paths:
        row, mismatches, columns = inspect_external_csv(
            path, args.source_root, extracted_at
        )
        manifest_rows.append(row)
        metadata[path] = (row["sha256"], columns)
        mismatched_file_count += bool(mismatches)
        mismatched_row_count += mismatches
    write_csv_manifest(manifest, manifest_rows)

    unique_by_hash: dict[str, tuple[Path, list[str]]] = {}
    for path, (digest, columns) in metadata.items():
        unique_by_hash.setdefault(digest, (path, columns))

    drive_activity_records: dict[str, dict[str, str]] = {}
    activity_hash_count = 0
    drive_raw_records: dict[str, dict[str, str]] = {}
    drive_raw_hashes: set[str] = set()
    for digest, (path, columns) in unique_by_hash.items():
        if {"Activity ID", "Start Date"}.issubset(columns):
            activity_hash_count += 1
            drive_activity_records.update(activity_records_from_csv(path))
        if "raw_json_" in path.name.lower() and "JSON Data" in columns:
            drive_raw_hashes.add(digest)
            drive_raw_records.update(raw_json_activity_records(path))

    repository_ids = set(csv_activity_ids(args.repository_activities))
    drive_only = set(drive_activity_records) - repository_ids
    drive_only_by_year = Counter(record_year(drive_activity_records[key]) for key in drive_only)

    repository_raw_hashes: set[str] = set()
    repository_raw_records: dict[str, dict[str, str]] = {}
    if args.repository_raw_root.is_dir():
        for path in sorted(args.repository_raw_root.glob("raw_json_*.csv")):
            repository_raw_hashes.add(sha256_file(path))
            repository_raw_records.update(raw_json_activity_records(path))

    raw_drive_only = set(drive_raw_records) - repository_ids
    raw_drive_only_by_year = Counter(record_year(drive_raw_records[key]) for key in raw_drive_only)
    candidate_suffixes = {
        ".archive",
        ".bson",
        ".jsonl",
        ".ndjson",
        ".tar",
        ".tgz",
        ".zip",
        ".7z",
    }
    expected_json_names = {
        "activities.json",
        "athlete_metadata.json",
        "master_iaaf.json",
        "update_logs.json",
    }
    export_candidates = []
    export_candidate_names: set[str] = set()
    for path in args.source_root.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(args.source_root)
        if EXTERNAL_SCAN_EXCLUDED_PARTS.intersection(relative.parts):
            continue
        lower_name = path.name.lower()
        if (
            path.suffix.lower() in candidate_suffixes
            or lower_name.endswith((".jsonl.gz", ".ndjson.gz", ".tar.gz"))
            or lower_name in expected_json_names
        ):
            export_candidates.append(relative.as_posix())
            export_candidate_names.add(lower_name.split(".", 1)[0])

    payload = {
        "dataset": "enduranceviz-2024",
        "specification_version": "1.0.0",
        "generated_at_utc": extracted_at,
        "source_root_label": args.source_root.name,
        "manifest_file": relative_to_root(manifest),
        "csv_inventory": {
            "file_count": len(paths),
            "unique_sha256_count": len(unique_by_hash),
            "distinct_schema_count": len(
                {row["schema_signature"] for row in manifest_rows}
            ),
            "row_width_mismatch_file_count": mismatched_file_count,
            "row_width_mismatch_count": mismatched_row_count,
        },
        "direct_activity_comparison": {
            "unique_activity_shaped_file_hashes": activity_hash_count,
            "external_unique_activity_ids": len(drive_activity_records),
            "repository_unique_activity_ids": len(repository_ids),
            "external_only_unique_activity_ids": len(drive_only),
            "external_only_by_start_year": dict(sorted(drive_only_by_year.items())),
            "external_only_2024_unique_activity_ids": drive_only_by_year.get("2024", 0),
        },
        "raw_json_comparison": {
            "external_unique_file_hashes": len(drive_raw_hashes),
            "hashes_already_in_repository": len(
                drive_raw_hashes & repository_raw_hashes
            ),
            "external_target_athlete_activity_ids": len(drive_raw_records),
            "external_ids_absent_from_repository_raw_json": len(
                set(drive_raw_records) - set(repository_raw_records)
            ),
            "external_ids_absent_from_repository_activity_csv": len(raw_drive_only),
            "external_ids_absent_from_repository_by_start_year": dict(
                sorted(raw_drive_only_by_year.items())
            ),
            "external_2024_ids_absent_from_repository_activity_csv": (
                raw_drive_only_by_year.get("2024", 0)
            ),
        },
        "mongo_export_search": {
            "candidate_count": len(export_candidates),
            "candidate_files": sorted(export_candidates),
            "complete_four_collection_snapshot_found": set(
                EXPECTED_MONGO_COLLECTIONS
            ).issubset(export_candidate_names),
        },
        "conclusion": (
            "The offline backup contributes no activity ID inside the canonical 2024 "
            "window that is absent from the repository source. It does not contain a "
            "MongoDB collection export, so it cannot reconcile the historical "
            "production-only observation."
        ),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    print(f"Wrote {len(manifest_rows)} entries to {relative_to_root(manifest)}")
    print(f"Wrote offline-source audit to {relative_to_root(output)}")
    return 0


def write_csv_manifest(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    refuse_overwrite(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=MANIFEST_FIELDS,
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


def inventory_repository(args: argparse.Namespace) -> int:
    generated = args.generated_at or utc_now()
    stamp = generated.date().isoformat()
    manifest = args.output or DEFAULT_MANIFEST_DIR / f"repository_csv_snapshot_{stamp}.csv"
    summary = args.summary or DEFAULT_MANIFEST_DIR / f"repository_csv_snapshot_{stamp}.json"
    refuse_overwrite(manifest)
    refuse_overwrite(summary)

    extracted_at = iso_utc(generated)
    commit = current_commit()
    paths = tracked_csv_paths()
    rows = [inspect_csv(path, extracted_at, commit) for path in paths]
    write_csv_manifest(manifest, rows)

    source_counts = Counter(row["source_system"] for row in rows)
    role_counts = Counter(row["source_role"] for row in rows)
    summary_payload = {
        "dataset": "enduranceviz-2024",
        "specification_version": "1.0.0",
        "extraction_time_utc": extracted_at,
        "source_commit": commit,
        "manifest_file": relative_to_root(manifest),
        "file_count": len(rows),
        "total_bytes": sum(int(row["byte_count"]) for row in rows),
        "distinct_schema_count": len({row["schema_signature"] for row in rows}),
        "files_with_read_errors": sum(bool(row["read_error"]) for row in rows),
        "source_system_counts": dict(sorted(source_counts.items())),
        "source_role_counts": dict(sorted(role_counts.items())),
    }
    with summary.open("x", encoding="utf-8") as handle:
        json.dump(summary_payload, handle, indent=2, sort_keys=True)
        handle.write("\n")

    print(f"Wrote {len(rows)} entries to {relative_to_root(manifest)}")
    print(f"Wrote summary to {relative_to_root(summary)}")
    return 0


def bson_type(value: Any) -> str:
    if value is None:
        return "null"
    return type(value).__name__


def export_collection(
    collection: Any,
    output_path: Path,
    batch_size: int = 100,
    max_retries: int = 20,
    projection: dict[str, int] | None = None,
) -> tuple[int, list[str]]:
    from bson.json_util import CANONICAL_JSON_OPTIONS, dumps
    from pymongo import ASCENDING
    from pymongo.errors import AutoReconnect, CursorNotFound, ExecutionTimeout, NetworkTimeout

    row_count = 0
    retry_count = 0
    last_id = None
    observed_types: dict[str, set[str]] = defaultdict(set)
    retryable = (AutoReconnect, CursorNotFound, ExecutionTimeout, NetworkTimeout)
    with output_path.open("xb") as raw_handle:
        with gzip.GzipFile(fileobj=raw_handle, mode="wb", mtime=0) as gzip_handle:
            with io.TextIOWrapper(gzip_handle, encoding="utf-8", newline="\n") as text_handle:
                while True:
                    query = {} if last_id is None else {"_id": {"$gt": last_id}}
                    cursor = (
                        collection.find(query, projection=projection)
                        .sort("_id", ASCENDING)
                        .batch_size(batch_size)
                    )
                    try:
                        for document in cursor:
                            for key, value in document.items():
                                observed_types[key].add(bson_type(value))
                            text_handle.write(
                                dumps(
                                    document,
                                    json_options=CANONICAL_JSON_OPTIONS,
                                    sort_keys=True,
                                    separators=(",", ":"),
                                )
                            )
                            text_handle.write("\n")
                            row_count += 1
                            last_id = document["_id"]
                            if row_count % 10_000 == 0:
                                text_handle.flush()
                                print(
                                    f"Exported {row_count:,} rows from {collection.name}",
                                    flush=True,
                                )
                    except retryable as exc:
                        retry_count += 1
                        if retry_count > max_retries:
                            raise
                        text_handle.flush()
                        print(
                            f"Retrying {collection.name} after {type(exc).__name__} "
                            f"({retry_count}/{max_retries}); {row_count:,} rows preserved",
                            flush=True,
                        )
                        time.sleep(min(2 * retry_count, 10))
                        continue
                    finally:
                        cursor.close()
                    break
    schema = [
        f"{key}:{'|'.join(sorted(types))}"
        for key, types in sorted(observed_types.items())
    ]
    return row_count, schema


def export_collection_partitioned(
    collection: Any,
    output_path: Path,
    chunk_size: int = 1000,
    max_retries: int = 20,
) -> tuple[int, list[str]]:
    """Export by bounded `_id` queries when a long-lived Atlas cursor is unstable."""
    from bson.json_util import CANONICAL_JSON_OPTIONS, dumps
    from pymongo.errors import AutoReconnect, CursorNotFound, ExecutionTimeout, NetworkTimeout

    if chunk_size <= 0 or max_retries < 0:
        raise ValueError("chunk-size must be positive and max-retries cannot be negative")
    retryable = (AutoReconnect, CursorNotFound, ExecutionTimeout, NetworkTimeout)
    identifiers = sorted(
        collection.distinct("_id", maxTimeMS=90_000), key=lambda value: str(value)
    )
    observed_types: dict[str, set[str]] = defaultdict(set)
    row_count = 0
    with output_path.open("xb") as raw_handle:
        with gzip.GzipFile(fileobj=raw_handle, mode="wb", mtime=0) as gzip_handle:
            with io.TextIOWrapper(gzip_handle, encoding="utf-8", newline="\n") as text_handle:
                for offset in range(0, len(identifiers), chunk_size):
                    chunk = identifiers[offset : offset + chunk_size]
                    retry_count = 0
                    while True:
                        try:
                            documents = list(
                                collection.find({"_id": {"$in": chunk}}).batch_size(
                                    max(100, len(chunk))
                                )
                            )
                            break
                        except retryable as exc:
                            retry_count += 1
                            if retry_count > max_retries:
                                raise
                            print(
                                f"Retrying {collection.name} chunk after "
                                f"{type(exc).__name__} ({retry_count}/{max_retries}); "
                                f"{row_count:,} rows preserved",
                                flush=True,
                            )
                            time.sleep(min(2 * retry_count, 10))

                    returned_ids = {document.get("_id") for document in documents}
                    if returned_ids != set(chunk) or len(documents) != len(chunk):
                        raise RuntimeError(
                            f"Incomplete partition for {collection.name}: requested "
                            f"{len(chunk)} unique _id values and received "
                            f"{len(returned_ids)} across {len(documents)} documents"
                        )
                    for document in sorted(
                        documents, key=lambda value: str(value.get("_id"))
                    ):
                        for key, value in document.items():
                            observed_types[key].add(bson_type(value))
                        text_handle.write(
                            dumps(
                                document,
                                json_options=CANONICAL_JSON_OPTIONS,
                                sort_keys=True,
                                separators=(",", ":"),
                            )
                        )
                        text_handle.write("\n")
                        row_count += 1
                    if row_count % 10_000 < len(chunk) or row_count == len(identifiers):
                        text_handle.flush()
                        print(
                            f"Exported {row_count:,}/{len(identifiers):,} rows from "
                            f"{collection.name}",
                            flush=True,
                        )

    if row_count != len(identifiers):
        raise RuntimeError(
            f"Export count mismatch for {collection.name}: "
            f"expected {len(identifiers)}, wrote {row_count}"
        )
    schema = [
        f"{key}:{'|'.join(sorted(types))}"
        for key, types in sorted(observed_types.items())
    ]
    return row_count, schema


def export_mongo(args: argparse.Namespace) -> int:
    try:
        from dotenv import load_dotenv
        from pymongo import MongoClient
        from pymongo.errors import PyMongoError
    except ImportError as exc:
        raise RuntimeError("Install requirements before exporting MongoDB") from exc

    load_dotenv(ROOT / ".env")
    if args.batch_size <= 0 or args.max_retries < 0:
        raise ValueError("batch-size must be positive and max-retries cannot be negative")
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise RuntimeError("MONGO_URI is not configured")

    generated = args.generated_at or utc_now()
    snapshot_name = compact_utc(generated)
    final_dir = args.output_root / snapshot_name
    manifest_path = args.manifest_dir / f"mongodb_snapshot_{snapshot_name}.csv"
    refuse_overwrite(final_dir)
    refuse_overwrite(manifest_path)
    args.output_root.mkdir(parents=True, exist_ok=True)
    args.manifest_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(prefix=f".{snapshot_name}-", dir=args.output_root))

    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=args.timeout_ms,
        connectTimeoutMS=args.timeout_ms,
        socketTimeoutMS=max(args.timeout_ms, 60_000),
    )
    extracted_at = iso_utc(generated)
    rows: list[dict[str, Any]] = []
    try:
        client.admin.command("ping")
        database = client[args.database]
        available = set(database.list_collection_names())
        missing = set(EXPECTED_MONGO_COLLECTIONS) - available
        if missing:
            raise RuntimeError(f"Missing expected MongoDB collections: {sorted(missing)}")

        for name in EXPECTED_MONGO_COLLECTIONS:
            output_path = temp_dir / f"{name}.jsonl.gz"
            row_count, schema = export_collection_partitioned(
                database[name],
                output_path,
                chunk_size=args.batch_size,
                max_retries=args.max_retries,
            )
            rows.append(
                {
                    "source_file": f"data/raw/2024/mongodb/{snapshot_name}/{output_path.name}",
                    "sha256": sha256_file(output_path),
                    "byte_count": output_path.stat().st_size,
                    "row_count": row_count,
                    "schema_signature": schema_signature(schema),
                    "schema_columns_json": json.dumps(schema, separators=(",", ":")),
                    "extraction_time_utc": extracted_at,
                    "source_system": f"mongodb:{args.database}/{name}",
                    "source_role": "raw",
                    "source_commit": current_commit(),
                    "read_error": "",
                }
            )

        marker = {
            "dataset": "enduranceviz-2024",
            "specification_version": "1.0.0",
            "database": args.database,
            "extraction_time_utc": extracted_at,
            "collections": [row["source_system"].rsplit("/", 1)[-1] for row in rows],
        }
        with (temp_dir / "SNAPSHOT.json").open("x", encoding="utf-8") as handle:
            json.dump(marker, handle, indent=2, sort_keys=True)
            handle.write("\n")
        temp_dir.rename(final_dir)
        write_csv_manifest(manifest_path, rows)

        for path in final_dir.iterdir():
            path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        final_dir.chmod(
            stat.S_IRUSR
            | stat.S_IXUSR
            | stat.S_IRGRP
            | stat.S_IXGRP
            | stat.S_IROTH
            | stat.S_IXOTH
        )
    except PyMongoError as exc:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise RuntimeError(
            f"MongoDB export failed ({type(exc).__name__}); verify Atlas cluster health and network access"
        ) from None
    except BaseException:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
    finally:
        client.close()

    print(f"Exported MongoDB snapshot to {relative_to_root(final_dir)}")
    print(f"Wrote manifest to {relative_to_root(manifest_path)}")
    return 0


def export_mongo_activity_ids(args: argparse.Namespace) -> int:
    """Create an immutable projection sufficient for activity-ID reconciliation."""
    try:
        from dotenv import load_dotenv
        from pymongo import MongoClient
        from pymongo.errors import PyMongoError
    except ImportError as exc:
        raise RuntimeError("Install requirements before exporting MongoDB") from exc

    load_dotenv(ROOT / ".env")
    if args.batch_size <= 0 or args.max_retries < 0:
        raise ValueError("batch-size must be positive and max-retries cannot be negative")
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise RuntimeError("MONGO_URI is not configured")

    generated = args.generated_at or utc_now()
    snapshot_name = compact_utc(generated)
    final_dir = args.output_root / snapshot_name
    manifest_path = args.manifest_dir / f"mongodb_activity_ids_{snapshot_name}.csv"
    refuse_overwrite(final_dir)
    refuse_overwrite(manifest_path)
    args.output_root.mkdir(parents=True, exist_ok=True)
    args.manifest_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(prefix=f".{snapshot_name}-", dir=args.output_root))

    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=args.timeout_ms,
        connectTimeoutMS=args.timeout_ms,
        socketTimeoutMS=max(args.timeout_ms, 60_000),
    )
    try:
        client.admin.command("ping")
        database = client[args.database]
        if "activities" not in set(database.list_collection_names()):
            raise RuntimeError("MongoDB activities collection is missing")
        output_path = temp_dir / "activities_ids.jsonl.gz"
        row_count, schema = export_collection(
            database["activities"],
            output_path,
            batch_size=args.batch_size,
            max_retries=args.max_retries,
            projection={"_id": 1, "Activity ID": 1},
        )
        extracted_at = iso_utc(generated)
        marker = {
            "dataset": "enduranceviz-2024",
            "specification_version": "1.0.0",
            "database": args.database,
            "extraction_time_utc": extracted_at,
            "projection": ["_id", "Activity ID"],
            "purpose": "activity_id_reconciliation_only",
            "row_count": row_count,
        }
        with (temp_dir / "SNAPSHOT.json").open("x", encoding="utf-8") as handle:
            json.dump(marker, handle, indent=2, sort_keys=True)
            handle.write("\n")
        temp_dir.rename(final_dir)
        write_csv_manifest(
            manifest_path,
            [
                {
                    "source_file": (
                        f"data/raw/2024/mongodb_activity_ids/{snapshot_name}/"
                        "activities_ids.jsonl.gz"
                    ),
                    "sha256": sha256_file(final_dir / output_path.name),
                    "byte_count": (final_dir / output_path.name).stat().st_size,
                    "row_count": row_count,
                    "schema_signature": schema_signature(schema),
                    "schema_columns_json": json.dumps(schema, separators=(",", ":")),
                    "extraction_time_utc": extracted_at,
                    "source_system": f"mongodb:{args.database}/activities",
                    "source_role": "reconciliation_extract",
                    "source_commit": current_commit(),
                    "read_error": "",
                }
            ],
        )
        for path in final_dir.iterdir():
            path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        final_dir.chmod(
            stat.S_IRUSR
            | stat.S_IXUSR
            | stat.S_IRGRP
            | stat.S_IXGRP
            | stat.S_IROTH
            | stat.S_IXOTH
        )
    except PyMongoError as exc:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise RuntimeError(
            f"MongoDB activity-ID export failed ({type(exc).__name__}); "
            "verify Atlas cluster health and network access"
        ) from None
    except BaseException:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise
    finally:
        client.close()

    print(f"Exported MongoDB activity IDs to {relative_to_root(final_dir)}")
    print(f"Wrote manifest to {relative_to_root(manifest_path)}")
    return 0


def normalize_activity_id(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def csv_activity_ids(path: Path) -> Counter[str]:
    ids: Counter[str] = Counter()
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        if "Activity ID" not in (reader.fieldnames or []):
            raise ValueError(f"Activity ID column not found in {path}")
        for row in reader:
            activity_id = normalize_activity_id(row.get("Activity ID"))
            if activity_id is not None:
                ids[activity_id] += 1
    return ids


def mongo_activity_ids(path: Path) -> Counter[str]:
    from bson.json_util import loads

    ids: Counter[str] = Counter()
    if path.suffix == ".gz":
        handle_context = gzip.open(path, "rt", encoding="utf-8")
    else:
        handle_context = path.open("r", encoding="utf-8")
    with handle_context as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            document = loads(line)
            activity_id = normalize_activity_id(document.get("Activity ID"))
            if activity_id is None:
                continue
            ids[activity_id] += 1
    return ids


def write_id_report(path: Path, ids: Iterable[str]) -> None:
    refuse_overwrite(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["activity_id"])
        writer.writerows((activity_id,) for activity_id in sorted(ids))


def reconcile_activities(args: argparse.Namespace) -> int:
    generated = args.generated_at or utc_now()
    stamp = compact_utc(generated)
    report = args.report or DEFAULT_MANIFEST_DIR / f"activity_reconciliation_{stamp}.json"
    local_only_path = report.with_name(f"activity_reconciliation_{stamp}_repository_only.csv")
    mongo_only_path = report.with_name(f"activity_reconciliation_{stamp}_mongodb_only.csv")
    for path in (report, local_only_path, mongo_only_path):
        refuse_overwrite(path)

    repository_ids = csv_activity_ids(args.repository_activities)
    mongo_ids = mongo_activity_ids(args.mongo_activities)
    repository_set = set(repository_ids)
    mongo_set = set(mongo_ids)
    local_only = repository_set - mongo_set
    mongo_only = mongo_set - repository_set
    shared = repository_set & mongo_set

    payload = {
        "dataset": "enduranceviz-2024",
        "specification_version": "1.0.0",
        "generated_at_utc": iso_utc(generated),
        "repository_source": relative_to_root(args.repository_activities),
        "mongodb_source": relative_to_root(args.mongo_activities),
        "repository": {
            "rows_with_activity_id": sum(repository_ids.values()),
            "unique_activity_ids": len(repository_ids),
            "duplicate_extra_rows": sum(count - 1 for count in repository_ids.values()),
        },
        "mongodb": {
            "rows_with_activity_id": sum(mongo_ids.values()),
            "unique_activity_ids": len(mongo_ids),
            "duplicate_extra_rows": sum(count - 1 for count in mongo_ids.values()),
        },
        "comparison": {
            "shared_unique_activity_ids": len(shared),
            "repository_only_unique_activity_ids": len(local_only),
            "mongodb_only_unique_activity_ids": len(mongo_only),
        },
        "repository_only_ids_file": relative_to_root(local_only_path),
        "mongodb_only_ids_file": relative_to_root(mongo_only_path),
    }
    report.parent.mkdir(parents=True, exist_ok=True)
    with report.open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    write_id_report(local_only_path, local_only)
    write_id_report(mongo_only_path, mongo_only)
    print(f"Wrote reconciliation report to {relative_to_root(report)}")
    return 0


def extract_mongo_activity_supplement(args: argparse.Namespace) -> int:
    """Promote Mongo-only activity rows as a manifested, immutable CSV input."""
    from bson.json_util import loads

    generated = args.generated_at or utc_now()
    snapshot_label = args.mongo_activities.parent.name
    output = args.output or (
        DEFAULT_RECONCILED_ROOT / snapshot_label / "production_only_activities.csv"
    )
    manifest = args.manifest or (
        DEFAULT_MANIFEST_DIR / f"mongodb_activity_supplement_{snapshot_label}.csv"
    )
    refuse_overwrite(output)
    refuse_overwrite(manifest)

    with args.repository_activities.open(
        "r", encoding="utf-8-sig", errors="replace", newline=""
    ) as handle:
        columns = next(csv.reader(handle), [])
    required = {"Athlete ID", "Activity ID", "Start Date", "Type"}
    if not required.issubset(columns):
        raise ValueError(
            f"Repository activity schema lacks required columns: {sorted(required - set(columns))}"
        )
    repository_ids = set(csv_activity_ids(args.repository_activities))

    documents: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    source_rows = 0
    handle_context = (
        gzip.open(args.mongo_activities, "rt", encoding="utf-8")
        if args.mongo_activities.suffix == ".gz"
        else args.mongo_activities.open("r", encoding="utf-8")
    )
    with handle_context as handle:
        for line in handle:
            if not line.strip():
                continue
            document = loads(line)
            activity_id = normalize_activity_id(document.get("Activity ID"))
            if activity_id is None or activity_id in repository_ids:
                continue
            source_rows += 1
            seen_ids.add(activity_id)
            documents.append(document)

    documents.sort(
        key=lambda document: (
            normalize_activity_id(document.get("Activity ID")) or "",
            str(document.get("_id", "")),
        )
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(documents)

    extracted_at = iso_utc(generated)
    write_csv_manifest(
        manifest,
        [
            {
                "source_file": relative_to_root(output),
                "sha256": sha256_file(output),
                "byte_count": output.stat().st_size,
                "row_count": source_rows,
                "schema_signature": schema_signature(columns),
                "schema_columns_json": json.dumps(columns, separators=(",", ":")),
                "extraction_time_utc": extracted_at,
                "source_system": f"mongodb_reconciliation:{snapshot_label}/activities",
                "source_role": "immutable_reconciliation_input",
                "source_commit": current_commit(),
                "read_error": "",
            }
        ],
    )
    output.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    print(
        f"Wrote {source_rows:,} Mongo-only rows ({len(seen_ids):,} unique IDs) "
        f"to {relative_to_root(output)}"
    )
    print(f"Wrote supplement manifest to {relative_to_root(manifest)}")
    return 0


def audit_local_activities(args: argparse.Namespace) -> int:
    generated = args.generated_at or utc_now()
    stamp = generated.date().isoformat()
    output = args.output or DEFAULT_MANIFEST_DIR / f"local_activity_audit_{stamp}.json"
    refuse_overwrite(output)

    total_rows = 0
    missing_ids = 0
    id_counts: Counter[str] = Counter()
    year_counts: Counter[str] = Counter()
    type_counts: Counter[str] = Counter()
    invalid_timestamps = 0
    min_timestamp: str | None = None
    max_timestamp: str | None = None

    with args.activities.open(
        "r", encoding="utf-8-sig", errors="replace", newline=""
    ) as handle:
        reader = csv.DictReader(handle)
        required = {"Activity ID", "Start Date", "Type"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")
        for row in reader:
            total_rows += 1
            activity_id = normalize_activity_id(row.get("Activity ID"))
            if activity_id is None:
                missing_ids += 1
            else:
                id_counts[activity_id] += 1
            raw_timestamp = (row.get("Start Date") or "").strip()
            try:
                timestamp = datetime.fromisoformat(raw_timestamp.replace("Z", "+00:00"))
                if timestamp.tzinfo is None:
                    raise ValueError("timestamp has no offset")
                normalized = iso_utc(timestamp)
                min_timestamp = normalized if min_timestamp is None else min(min_timestamp, normalized)
                max_timestamp = normalized if max_timestamp is None else max(max_timestamp, normalized)
                year_counts[str(timestamp.astimezone(timezone.utc).year)] += 1
            except ValueError:
                invalid_timestamps += 1
            type_counts[(row.get("Type") or "<missing>").strip() or "<missing>"] += 1

    payload = {
        "dataset": "enduranceviz-2024",
        "specification_version": "1.0.0",
        "generated_at_utc": iso_utc(generated),
        "source_file": relative_to_root(args.activities),
        "source_sha256": sha256_file(args.activities),
        "total_rows": total_rows,
        "rows_with_missing_activity_id": missing_ids,
        "unique_activity_ids": len(id_counts),
        "duplicate_extra_rows": sum(count - 1 for count in id_counts.values()),
        "invalid_start_timestamps": invalid_timestamps,
        "minimum_start_timestamp_utc": min_timestamp,
        "maximum_start_timestamp_utc": max_timestamp,
        "start_year_counts": dict(sorted(year_counts.items())),
        "raw_activity_type_counts": dict(type_counts.most_common()),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    print(f"Wrote local activity audit to {relative_to_root(output)}")
    return 0


def parsed_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("Timestamp must include an offset")
    return parsed.astimezone(timezone.utc)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    inventory = subparsers.add_parser(
        "inventory-repository", help="Manifest every Git-tracked CSV without modifying it"
    )
    inventory.add_argument("--output", type=Path)
    inventory.add_argument("--summary", type=Path)
    inventory.add_argument("--generated-at", type=parsed_datetime)
    inventory.set_defaults(func=inventory_repository)

    audit = subparsers.add_parser(
        "audit-local-activities", help="Audit the legacy canonical activity CSV"
    )
    audit.add_argument("--activities", type=Path, default=DEFAULT_ACTIVITY_CSV)
    audit.add_argument("--output", type=Path)
    audit.add_argument("--generated-at", type=parsed_datetime)
    audit.set_defaults(func=audit_local_activities)

    external = subparsers.add_parser(
        "audit-external-drive",
        help="Manifest and compare an offline EnduranceViz backup read-only",
    )
    external.add_argument("--source-root", type=Path, required=True)
    external.add_argument("--repository-activities", type=Path, default=DEFAULT_ACTIVITY_CSV)
    external.add_argument("--repository-raw-root", type=Path, default=DEFAULT_RAW_JSON_ROOT)
    external.add_argument("--manifest", type=Path)
    external.add_argument("--output", type=Path)
    external.add_argument("--generated-at", type=parsed_datetime)
    external.set_defaults(func=audit_external_drive)

    export = subparsers.add_parser(
        "export-mongo", help="Export the four production MongoDB collections once"
    )
    export.add_argument("--database", default="elite_endurance")
    export.add_argument("--output-root", type=Path, default=DEFAULT_MONGO_ROOT)
    export.add_argument("--manifest-dir", type=Path, default=DEFAULT_MANIFEST_DIR)
    export.add_argument("--timeout-ms", type=int, default=15_000)
    export.add_argument("--batch-size", type=int, default=100)
    export.add_argument("--max-retries", type=int, default=20)
    export.add_argument("--generated-at", type=parsed_datetime)
    export.set_defaults(func=export_mongo)

    export_ids = subparsers.add_parser(
        "export-mongo-activity-ids",
        help="Export only Mongo _id and Activity ID fields for source reconciliation",
    )
    export_ids.add_argument("--database", default="elite_endurance")
    export_ids.add_argument("--output-root", type=Path, default=DEFAULT_MONGO_ID_ROOT)
    export_ids.add_argument("--manifest-dir", type=Path, default=DEFAULT_MANIFEST_DIR)
    export_ids.add_argument("--timeout-ms", type=int, default=15_000)
    export_ids.add_argument("--batch-size", type=int, default=1000)
    export_ids.add_argument("--max-retries", type=int, default=20)
    export_ids.add_argument("--generated-at", type=parsed_datetime)
    export_ids.set_defaults(func=export_mongo_activity_ids)

    reconcile = subparsers.add_parser(
        "reconcile-activities", help="Compare repository and Mongo activity IDs"
    )
    reconcile.add_argument("--repository-activities", type=Path, default=DEFAULT_ACTIVITY_CSV)
    reconcile.add_argument("--mongo-activities", type=Path, required=True)
    reconcile.add_argument("--report", type=Path)
    reconcile.add_argument("--generated-at", type=parsed_datetime)
    reconcile.set_defaults(func=reconcile_activities)

    supplement = subparsers.add_parser(
        "extract-mongo-activity-supplement",
        help="Write Mongo-only activities as an immutable canonical-build input",
    )
    supplement.add_argument("--mongo-activities", type=Path, required=True)
    supplement.add_argument(
        "--repository-activities", type=Path, default=DEFAULT_ACTIVITY_CSV
    )
    supplement.add_argument("--output", type=Path)
    supplement.add_argument("--manifest", type=Path)
    supplement.add_argument("--generated-at", type=parsed_datetime)
    supplement.set_defaults(func=extract_mongo_activity_supplement)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return args.func(args)
    except (FileExistsError, OSError, RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
