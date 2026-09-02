from __future__ import annotations

import argparse
import csv
import gzip
import json
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timezone
from pathlib import Path

import yaml
from bson.json_util import CANONICAL_JSON_OPTIONS, dumps
from pymongo.errors import NetworkTimeout

from scripts import snapshot_2024


ROOT = Path(__file__).resolve().parents[1]


class SnapshotContractTests(unittest.TestCase):
    def test_contract_has_utc_half_open_2024_window(self) -> None:
        with (ROOT / "config" / "snapshot_2024.yaml").open(encoding="utf-8") as handle:
            contract = yaml.safe_load(handle)

        self.assertEqual(contract["dataset"]["specification_version"], "1.0.0")
        self.assertEqual(contract["window"]["start_inclusive_utc"], "2024-01-01T00:00:00Z")
        self.assertEqual(contract["window"]["end_exclusive_utc"], "2025-01-01T00:00:00Z")
        self.assertEqual(contract["window"]["activity_assignment"], "start_timestamp")
        self.assertEqual(contract["units"]["distance"], "meter")
        self.assertEqual(contract["units"]["duration"], "second")

    def test_activity_mapping_covers_every_current_non_null_type(self) -> None:
        with (ROOT / "config" / "snapshot_2024.yaml").open(encoding="utf-8") as handle:
            contract = yaml.safe_load(handle)
        mapped = {
            raw_type
            for category, mapping in contract["activity_categories"].items()
            if category != "Other"
            for raw_type in mapping
        }

        observed: set[str] = set()
        with (ROOT / "indiv_activities_full.csv").open(
            encoding="utf-8-sig", errors="replace", newline=""
        ) as handle:
            for row in csv.DictReader(handle):
                if row["Type"]:
                    observed.add(row["Type"])

        # Unmapped types deliberately fall into Other; mapped types must be observed.
        self.assertTrue(mapped.issubset(observed))
        self.assertIn("WeightTraining", observed - mapped)


class SnapshotToolTests(unittest.TestCase):
    def test_normalize_activity_id(self) -> None:
        self.assertEqual(snapshot_2024.normalize_activity_id("123.0"), "123")
        self.assertEqual(snapshot_2024.normalize_activity_id(123), "123")
        self.assertIsNone(snapshot_2024.normalize_activity_id("nan"))

    def test_reconciliation_reports_duplicates_and_source_differences(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            repository_csv = temp_path / "activities.csv"
            mongo_jsonl = temp_path / "activities.jsonl.gz"
            report = temp_path / "report.json"

            with repository_csv.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["Activity ID"])
                writer.writeheader()
                writer.writerows(
                    [{"Activity ID": "1"}, {"Activity ID": "1"}, {"Activity ID": "2"}]
                )
            with gzip.open(mongo_jsonl, "wt", encoding="utf-8") as handle:
                for activity_id in ("1", "3", "3"):
                    handle.write(
                        dumps(
                            {"Activity ID": activity_id},
                            json_options=CANONICAL_JSON_OPTIONS,
                        )
                    )
                    handle.write("\n")

            args = argparse.Namespace(
                repository_activities=repository_csv,
                mongo_activities=mongo_jsonl,
                report=report,
                generated_at=datetime(2026, 9, 2, tzinfo=timezone.utc),
            )
            self.assertEqual(snapshot_2024.reconcile_activities(args), 0)
            payload = json.loads(report.read_text())

            self.assertEqual(payload["repository"]["duplicate_extra_rows"], 1)
            self.assertEqual(payload["mongodb"]["duplicate_extra_rows"], 1)
            self.assertEqual(payload["comparison"]["shared_unique_activity_ids"], 1)
            self.assertEqual(payload["comparison"]["repository_only_unique_activity_ids"], 1)
            self.assertEqual(payload["comparison"]["mongodb_only_unique_activity_ids"], 1)

    def test_manifest_outputs_are_immutable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "manifest.csv"
            path.write_text("already exists", encoding="utf-8")
            with self.assertRaises(FileExistsError):
                snapshot_2024.write_csv_manifest(path, [])

    def test_manifest_reads_oversized_legacy_csv_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "raw_json.csv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["Athlete ID", "JSON"])
                writer.writerow(["123", "x" * 200_000])

            result = snapshot_2024.inspect_csv(
                path,
                "2026-09-02T00:00:00Z",
                "test-commit",
            )

            self.assertEqual(result["row_count"], 1)
            self.assertEqual(result["read_error"], "")

    def test_mongo_export_resumes_after_transient_cursor_timeout(self) -> None:
        class FakeCursor:
            def __init__(self, rows, fail_after_first=False):
                self.rows = rows
                self.fail_after_first = fail_after_first

            def sort(self, *_):
                return self

            def batch_size(self, *_):
                return self

            def __iter__(self):
                for index, row in enumerate(self.rows):
                    if self.fail_after_first and index == 1:
                        raise NetworkTimeout("fixture timeout")
                    yield row

            def close(self):
                return None

        class FakeCollection:
            name = "fixture"

            def __init__(self):
                self.calls = []

            def find(self, query):
                self.calls.append(query)
                if len(self.calls) == 1:
                    return FakeCursor([{"_id": 1, "value": "a"}, {"_id": 2, "value": "b"}], True)
                return FakeCursor([{"_id": 2, "value": "b"}, {"_id": 3, "value": "c"}])

        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "fixture.jsonl.gz"
            collection = FakeCollection()
            with patch("scripts.snapshot_2024.time.sleep"):
                row_count, _ = snapshot_2024.export_collection(
                    collection, path, batch_size=1, max_retries=1
                )
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                ids = [json.loads(line)["_id"]["$numberInt"] for line in handle]

        self.assertEqual(row_count, 3)
        self.assertEqual(ids, ["1", "2", "3"])
        self.assertEqual(collection.calls[1], {"_id": {"$gt": 1}})


if __name__ == "__main__":
    unittest.main()
