from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from scripts.initialize_2024_database import initialize_database


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_TABLES = {
    "dataset_builds",
    "import_manifest",
    "athletes",
    "athlete_external_accounts",
    "performances_2024",
    "activities_2024",
    "data_coverage_2024",
    "weekly_training_2024",
    "athlete_summary_2024",
    "quarantined_records",
}


class CanonicalSchemaTests(unittest.TestCase):
    def create_database(self, directory: str) -> Path:
        database = Path(directory) / "test.duckdb"
        initialize_database(ROOT / "schema" / "2024.sql", database)
        return database

    def test_schema_creates_every_canonical_table(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.create_database(temp)
            with duckdb.connect(str(database)) as connection:
                tables = {
                    row[0]
                    for row in connection.execute("SHOW TABLES").fetchall()
                }
            self.assertEqual(tables, EXPECTED_TABLES)

    def test_external_account_is_unique_per_provider(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.create_database(temp)
            with duckdb.connect(str(database)) as connection:
                connection.execute(
                    """
                    INSERT INTO athletes VALUES
                    ('00000000-0000-0000-0000-000000000001', 'One', 'one', NULL,
                     'CAN', 'unknown', 'resolved', 'fixture', now(), now()),
                    ('00000000-0000-0000-0000-000000000002', 'Two', 'two', NULL,
                     'USA', 'unknown', 'resolved', 'fixture', now(), now())
                    """
                )
                connection.execute(
                    """
                    INSERT INTO athlete_external_accounts VALUES
                    ('strava', '123', '00000000-0000-0000-0000-000000000001',
                     'One', 'fixture', 'resolved', 'fixture.csv', 1)
                    """
                )
                with self.assertRaises(duckdb.ConstraintException):
                    connection.execute(
                        """
                        INSERT INTO athlete_external_accounts VALUES
                        ('strava', '123', '00000000-0000-0000-0000-000000000002',
                         'Two', 'fixture', 'resolved', 'fixture.csv', 2)
                        """
                    )

    def test_activity_requires_existing_identity_and_2024_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.create_database(temp)
            with duckdb.connect(str(database)) as connection:
                with self.assertRaises(duckdb.ConstraintException):
                    connection.execute(
                        """
                        INSERT INTO activities_2024 (
                            activity_id, athlete_id, provider, external_account_id,
                            provider_activity_type, activity_category, start_at_utc,
                            week_start_utc, source_file, source_row_number
                        ) VALUES (
                            'activity-1',
                            '00000000-0000-0000-0000-000000000099',
                            'strava', 'missing', 'Run', 'Run',
                            TIMESTAMPTZ '2025-01-01T00:00:00Z', DATE '2024-12-30',
                            'fixture.csv', 1
                        )
                        """
                    )

    def test_initializer_refuses_to_overwrite_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = self.create_database(temp)
            with self.assertRaises(FileExistsError):
                initialize_database(ROOT / "schema" / "2024.sql", database)


if __name__ == "__main__":
    unittest.main()
