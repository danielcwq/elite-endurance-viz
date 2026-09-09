"""Regression fixtures for ambiguous empties and sparse posting."""
import unittest

import duckdb

from enduranceviz.observability import ATHLETE_POSTING_SQL, WEEK_EVIDENCE_SQL


class ObservabilityTests(unittest.TestCase):
    def setUp(self):
        self.connection = duckdb.connect(':memory:')
        self.connection.execute("""
            CREATE TABLE data_coverage_2024 (
                athlete_id VARCHAR, week_start_utc DATE, is_partial_window BOOLEAN,
                coverage_status VARCHAR, collection_error_code VARCHAR,
                evidence_source VARCHAR, observation_status VARCHAR
            );
            CREATE TABLE activities_2024 (
                athlete_id VARCHAR, week_start_utc DATE, activity_category VARCHAR, distance_meters DOUBLE
            );
            CREATE TABLE athlete_directory_2024 (
                athlete_id VARCHAR, display_name VARCHAR, official_name VARCHAR,
                primary_discipline VARCHAR, gender VARCHAR, default_cohort_eligible BOOLEAN
            );
            INSERT INTO athlete_directory_2024 VALUES ('one', 'Fixture', 'Fixture', '800m', 'female', true);
            INSERT INTO data_coverage_2024 VALUES
                ('one', '2024-01-01', false, 'complete', NULL, 'legacy.csv', 'observed'),
                ('one', '2024-01-08', false, 'complete', NULL, 'legacy.csv', 'observed'),
                ('one', '2024-01-15', false, 'observed_with_warning', 'SUMMARY_ACTIVITY_CONTRADICTION', 'legacy.csv', 'observed'),
                ('one', '2024-01-22', false, 'missing', NULL, NULL, 'missing'),
                ('one', '2024-01-29', false, 'missing', NULL, NULL, 'missing'),
                ('one', '2024-02-05', false, 'complete', NULL, 'legacy.csv', 'observed'),
                ('one', '2024-12-30', true, 'partial_window', NULL, 'legacy.csv', 'observed');
            INSERT INTO activities_2024 VALUES
                ('one', '2024-01-15', 'Run', 5000),
                ('one', '2024-01-22', 'Run', 1000),
                ('one', '2024-01-22', 'Run', NULL),
                ('one', '2024-02-05', 'Ride', 20000),
                ('one', '2024-12-30', 'Run', 2000);
        """)

    def tearDown(self):
        self.connection.close()

    def rows(self, query):
        result = self.connection.execute(query)
        names = [x[0] for x in result.description]
        return [dict(zip(names, row)) for row in result.fetchall()]

    def test_empty_legacy_records_do_not_prove_success_or_training_zero(self):
        rows = self.rows(WEEK_EVIDENCE_SQL)
        first = next(r for r in rows if str(r['week_start_utc']) == '2024-01-01')
        self.assertEqual(first['evidence_state'], 'ambiguous_empty_record')
        self.assertIsNone(first['known_run_distance_meters'])
        self.assertEqual(first['posted_runs'], 0)

    def test_warning_and_missing_summary_do_not_discard_recorded_runs(self):
        rows = {str(r['week_start_utc']): r for r in self.rows(WEEK_EVIDENCE_SQL)}
        self.assertEqual(rows['2024-01-15']['evidence_state'], 'source_warning')
        self.assertEqual(rows['2024-01-15']['posted_runs'], 1)
        self.assertEqual(rows['2024-01-22']['evidence_state'], 'activity_without_weekly_record')
        self.assertEqual(rows['2024-01-22']['posted_runs'], 2)
        self.assertEqual(rows['2024-01-22']['runs_missing_distance'], 1)
        self.assertEqual(rows['2024-01-29']['evidence_state'], 'no_collection_evidence')

    def test_posting_counts_keep_partial_week_and_non_run_distinctions(self):
        row = self.rows(ATHLETE_POSTING_SQL)[0]
        self.assertEqual(row['recorded_runs_2024'], 4)
        self.assertEqual(row['weeks_with_runs'], 2)
        self.assertEqual(row['weeks_with_activities'], 3)
        self.assertEqual(row['longest_no_recorded_run_gap'], 2)
        self.assertEqual(row['ambiguous_empty_weeks'], 2)
        self.assertTrue(row['legacy_p0_eligible'])
        self.assertEqual(row['warning_weeks'], 1)
        self.assertEqual(row['weeks_with_missing_run_distance'], 1)


if __name__ == '__main__':
    unittest.main()
