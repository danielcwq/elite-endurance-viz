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

    def test_mongo_supplement_preserves_only_repository_missing_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            repository_csv = temp_path / "activities.csv"
            mongo_jsonl = temp_path / "activities.jsonl.gz"
            output = temp_path / "supplement.csv"
            manifest = temp_path / "manifest.csv"
            columns = ["Athlete ID", "Activity ID", "Start Date", "Type"]
            with repository_csv.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=columns)
                writer.writeheader()
                writer.writerow(
                    {
                        "Athlete ID": "10",
                        "Activity ID": "1",
                        "Start Date": "2024-01-01T00:00:00Z",
                        "Type": "Run",
                    }
                )
            with gzip.open(mongo_jsonl, "wt", encoding="utf-8") as handle:
                for mongo_id, activity_id in ((1, "1"), (2, "2"), (3, "2")):
                    handle.write(
                        dumps(
                            {
                                "_id": mongo_id,
                                "Athlete ID": "10",
                                "Activity ID": activity_id,
                                "Start Date": "2024-01-02T00:00:00Z",
                                "Type": "Run",
                            },
                            json_options=CANONICAL_JSON_OPTIONS,
                        )
                    )
                    handle.write("\n")
            args = argparse.Namespace(
                repository_activities=repository_csv,
                mongo_activities=mongo_jsonl,
                output=output,
                manifest=manifest,
                generated_at=datetime(2026, 9, 2, tzinfo=timezone.utc),
            )

            self.assertEqual(snapshot_2024.extract_mongo_activity_supplement(args), 0)
            with output.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            with manifest.open(encoding="utf-8", newline="") as handle:
                manifest_row = next(csv.DictReader(handle))

            self.assertEqual(len(rows), 2)
            self.assertEqual({row["Activity ID"] for row in rows}, {"2"})
            self.assertEqual(manifest_row["row_count"], "2")

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

    def test_external_activity_reader_recovers_headerless_and_glued_tail(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "mixed.csv"
            old_columns = [
                "Serial",
                "Athlete ID",
                "Athlete Name",
                "Activity ID",
                "Activity Name",
                "Description",
                "Start Date",
                "Elapsed Time",
                "Type",
                "Location",
                "Pace (min/mi)",
                "Pace (min/km)",
                "Time (min)",
                "Distance (km)",
                "Activity Time (s)",
                "Time",
            ]
            old_row = [
                "1",
                "10",
                "Runner One",
                "100",
                "Run",
                "",
                "2024-01-01T00:00:00Z",
                "60",
                "Run",
                "",
                "",
                "5",
                "1",
                "1",
                "60",
            ]
            processed_row = [
                "20",
                "Runner Two",
                "200",
                "Run",
                "",
                "2025-01-01T00:00:00Z",
                "60",
                "Run",
                "",
                "1",
                "5",
                "1m",
            ]
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(old_columns)
                writer.writerow(old_row + processed_row)
                writer.writerow(processed_row)

            records = snapshot_2024.activity_records_from_csv(path)

            self.assertEqual(set(records), {"100", "200"})
            self.assertEqual(records["100"]["start_date"], "2024-01-01T00:00:00Z")
            self.assertEqual(records["200"]["start_date"], "2025-01-01T00:00:00Z")

    def test_raw_json_reader_keeps_only_target_athlete(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "raw_json_fixture.csv"
            payload = {
                "preFetchedEntries": [
                    {
                        "activity": {
                            "id": 100,
                            "startDate": "2024-01-01T00:00:00Z",
                            "type": "Run",
                            "athlete": {"athleteId": 10, "athleteName": "Runner One"},
                        }
                    },
                    {
                        "activity": {
                            "id": 999,
                            "startDate": "2024-01-02T00:00:00Z",
                            "type": "Run",
                            "athlete": {"athleteId": 99, "athleteName": "Someone Else"},
                        }
                    },
                ]
            }
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle, fieldnames=["Athlete ID", "Name", "JSON Data"]
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "Athlete ID": "10",
                        "Name": " runner one ",
                        "JSON Data": json.dumps(payload),
                    }
                )

            records = snapshot_2024.raw_json_activity_records(path)

            self.assertEqual(set(records), {"100"})

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

            def find(self, query, projection=None):
                self.calls.append((query, projection))
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
        self.assertEqual(collection.calls[1], ({"_id": {"$gt": 1}}, None))

    def test_partitioned_export_uses_bounded_id_queries(self) -> None:
        class FakeCursor:
            def __init__(self, rows):
                self.rows = rows

            def batch_size(self, *_):
                return self

            def __iter__(self):
                return iter(self.rows)

        class FakeCollection:
            name = "fixture"

            def __init__(self):
                self.documents = [
                    {"_id": 3, "value": "c"},
                    {"_id": 1, "value": "a"},
                    {"_id": 2, "value": "b"},
                ]
                self.queries = []

            def distinct(self, key, **_):
                self.asserted_key = key
                return [document[key] for document in self.documents]

            def find(self, query):
                self.queries.append(query)
                ids = set(query["_id"]["$in"])
                return FakeCursor(
                    [document for document in self.documents if document["_id"] in ids]
                )

        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "fixture.jsonl.gz"
            collection = FakeCollection()
            row_count, _ = snapshot_2024.export_collection_partitioned(
                collection, path, chunk_size=2
            )
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                ids = [json.loads(line)["_id"]["$numberInt"] for line in handle]

        self.assertEqual(row_count, 3)
        self.assertEqual(ids, ["1", "2", "3"])
        self.assertEqual(collection.asserted_key, "_id")
        self.assertEqual(len(collection.queries), 2)


if __name__ == "__main__":
    unittest.main()
