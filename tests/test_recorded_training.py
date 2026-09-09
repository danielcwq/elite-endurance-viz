import unittest

import duckdb

from enduranceviz.recorded_training import ATHLETE_RECORDED_TRAINING_SQL, RECORDED_WEEK_METRICS_SQL


class RecordedTrainingTests(unittest.TestCase):
    def setUp(self):
        self.c = duckdb.connect(':memory:')
        self.c.execute("""
            CREATE TABLE athlete_directory_2024 AS SELECT * FROM (VALUES
                ('a', '800m', 'female'), ('b', '5000m', 'male'), ('c', '5000m', 'female'))
                AS t(athlete_id, primary_discipline, gender);
            CREATE TABLE data_coverage_2024 AS SELECT d.athlete_id, w::DATE AS week_start_utc,
                w >= DATE '2024-12-30' AS is_partial_window, 'unknown' AS coverage_status,
                NULL::VARCHAR AS collection_error_code, NULL::VARCHAR AS evidence_source,
                'unknown' AS observation_status
                FROM athlete_directory_2024 d CROSS JOIN
                generate_series(DATE '2024-01-01', DATE '2024-12-30', INTERVAL '1 week') t(w);
            CREATE TABLE activities_2024 (athlete_id VARCHAR, week_start_utc DATE,
                activity_category VARCHAR, distance_meters DOUBLE);
            INSERT INTO activities_2024 VALUES
                ('a','2024-01-01','Run',5000), ('a','2024-01-01','Run',NULL),
                ('a','2024-01-08','Run',0), ('a','2024-01-15','Run',10000),
                ('a','2024-12-30','Run',99999), ('b','2024-01-01','Ride',20000);
            UPDATE data_coverage_2024 SET collection_error_code='SUMMARY_ACTIVITY_CONTRADICTION',
                coverage_status='observed_with_warning' WHERE athlete_id='a' AND week_start_utc='2024-01-15';
        """)

    def tearDown(self):
        self.c.close()

    def test_missing_measurement_does_not_turn_into_complete_week_distance(self):
        row = self.c.execute(f"SELECT posted_runs, known_run_distance_meters, recorded_week_run_distance_meters FROM ({RECORDED_WEEK_METRICS_SQL}) WHERE athlete_id='a' AND week_start_utc='2024-01-01'").fetchone()
        self.assertEqual(row, (2, 5000, None))

    def test_real_zero_is_preserved_but_no_run_week_is_null(self):
        rows = self.c.execute(f"SELECT athlete_id, recorded_week_run_distance_meters FROM ({RECORDED_WEEK_METRICS_SQL}) WHERE (athlete_id='a' AND week_start_utc='2024-01-08') OR (athlete_id='b' AND week_start_utc='2024-01-01') ORDER BY 1").fetchall()
        self.assertEqual(rows, [('a', 0), ('b', None)])

    def test_athlete_summary_keeps_partial_records_and_separate_denominators(self):
        row = self.c.execute(f"SELECT recorded_runs_2024, recorded_runs_full_weeks, recorded_run_weeks, distance_measured_weeks, distance_unavailable_run_weeks, source_warning_run_weeks, median_recorded_week_run_count, median_recorded_week_run_distance_meters FROM ({ATHLETE_RECORDED_TRAINING_SQL}) WHERE athlete_id='a'").fetchone()
        self.assertEqual(row, (5, 4, 3, 2, 1, 1, 1, 5000))

    def test_no_records_athlete_stays_in_inventory_without_training_zero(self):
        row = self.c.execute(f"SELECT recorded_runs_2024, median_recorded_week_run_count, median_recorded_week_run_distance_meters FROM ({ATHLETE_RECORDED_TRAINING_SQL}) WHERE athlete_id='c'").fetchone()
        self.assertEqual(row, (0, None, None))

    def test_invalid_distances_keep_run_counts_but_block_weekly_distance(self):
        self.c.execute("INSERT INTO activities_2024 VALUES ('b','2024-01-08','Run',-10), ('b','2024-01-15','Run','Infinity'::DOUBLE)")
        rows = self.c.execute(f"SELECT posted_runs, runs_invalid_distance, recorded_week_run_distance_meters FROM ({RECORDED_WEEK_METRICS_SQL}) WHERE athlete_id='b' AND posted_runs>0").fetchall()
        self.assertEqual(rows, [(1, 1, None), (1, 1, None)])


if __name__ == '__main__':
    unittest.main()
