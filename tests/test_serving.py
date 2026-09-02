from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from enduranceviz.serving import ServingRepository


class ServingRepositoryTests(unittest.TestCase):
    def test_search_uses_stable_id_and_activity_query_is_paginated(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "serving.duckdb"
            with duckdb.connect(str(database)) as connection:
                connection.execute(
                    """
                    CREATE TABLE athlete_directory_2024 (
                        athlete_id UUID, official_name VARCHAR, display_name VARCHAR,
                        nationality_code VARCHAR, primary_discipline VARCHAR,
                        coverage_status VARCHAR, coverage_score DOUBLE,
                        default_cohort_eligible BOOLEAN, total_activity_count INTEGER,
                        total_run_distance_meters DOUBLE, total_run_duration_seconds DOUBLE,
                        average_run_distance_per_observed_week_meters DOUBLE,
                        average_run_distance_per_calendar_week_meters DOUBLE,
                        weighted_run_pace_seconds_per_kilometer DOUBLE,
                        observed_weeks INTEGER, dataset_version VARCHAR,
                        computed_at_utc TIMESTAMPTZ, gender VARCHAR
                    );
                    INSERT INTO athlete_directory_2024 VALUES (
                        '00000000-0000-0000-0000-000000000001', 'Runner One', 'RUNNER ONE',
                        'CAN', '5000m', 'high', 95, true, 2, 3000, 900,
                        1500, 57.4, 300, 52, 'fixture', now(), 'unknown'
                    );
                    CREATE TABLE dataset_builds (
                        specification_version VARCHAR, completed_at_utc TIMESTAMPTZ,
                        source_commit VARCHAR, status VARCHAR
                    );
                    INSERT INTO dataset_builds VALUES ('fixture', now(), 'abc', 'succeeded');
                    CREATE TABLE athletes (athlete_id UUID, nationality_code VARCHAR);
                    INSERT INTO athletes VALUES ('00000000-0000-0000-0000-000000000001', 'CAN');
                    CREATE TABLE athlete_external_accounts (
                        athlete_id UUID, provider VARCHAR, external_account_id VARCHAR,
                        provider_display_name VARCHAR
                    );
                    INSERT INTO athlete_external_accounts VALUES (
                        '00000000-0000-0000-0000-000000000001', 'strava', '10', 'Runner One'
                    );
                    CREATE TABLE performances_2024 (
                        athlete_id UUID, discipline VARCHAR, mark_text VARCHAR,
                        performance_date DATE, location VARCHAR, results_score INTEGER,
                        is_season_best BOOLEAN
                    );
                    CREATE TABLE activities_2024 (
                        athlete_id UUID, activity_id VARCHAR, activity_name VARCHAR,
                        description VARCHAR, provider_activity_type VARCHAR,
                        activity_category VARCHAR, start_at_utc TIMESTAMPTZ,
                        distance_meters DOUBLE, elapsed_seconds DOUBLE,
                        moving_seconds DOUBLE, pace_seconds_per_kilometer DOUBLE,
                        location VARCHAR, quality_status VARCHAR, quality_flags VARCHAR[]
                    );
                    INSERT INTO activities_2024 VALUES
                    ('00000000-0000-0000-0000-000000000001', '2', 'Second', NULL, 'Run', 'Run', now(), 2000, 600, 600, 300, NULL, 'valid', []),
                    ('00000000-0000-0000-0000-000000000001', '1', 'First', NULL, 'Run', 'Run', now() - INTERVAL 1 DAY, 1000, 300, 300, 300, NULL, 'valid', []);
                    """
                )
            repository = ServingRepository(database)
            result = repository.search_athletes("runner")
            self.assertEqual(result[0]["athlete_id"], "00000000-0000-0000-0000-000000000001")
            page = repository.activities(result[0]["athlete_id"], page=1, page_size=1)
            self.assertEqual(len(page["rows"]), 1)
            self.assertTrue(page["has_next"])
            self.assertNotIn("description_that_was_not_projected", page["rows"][0])


if __name__ == "__main__":
    unittest.main()
