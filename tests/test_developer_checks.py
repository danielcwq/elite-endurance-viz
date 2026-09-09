import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from enduranceviz.recorded_training import RECORDED_WEEK_METRICS_SQL
from enduranceviz.serving import ServingRepository
from scripts import check_serving_2024, dev


class RecordedWeekCheckTests(unittest.TestCase):
    def setUp(self):
        self.c = duckdb.connect(':memory:')
        self.c.execute('''
            CREATE TABLE athletes AS SELECT 'synthetic' AS athlete_id;
            CREATE TABLE data_coverage_2024 AS SELECT athlete_id, w::DATE AS week_start_utc,
                w=DATE '2024-12-30' AS is_partial_window, 'unknown' AS coverage_status,
                NULL::VARCHAR AS collection_error_code, NULL::VARCHAR AS evidence_source,
                'unknown' AS observation_status FROM athletes CROSS JOIN
                generate_series(DATE '2024-01-01', DATE '2024-12-30', INTERVAL '1 week') t(w);
            CREATE TABLE activities_2024 (athlete_id VARCHAR, week_start_utc DATE,
                activity_category VARCHAR, distance_meters DOUBLE, start_at_utc TIMESTAMPTZ);
            INSERT INTO activities_2024 VALUES
                ('synthetic','2024-01-01','Run',5000,'2024-01-02T01:00:00Z'),
                ('synthetic','2024-01-08','Run',NULL,'2024-01-08T23:00:00Z'),
                ('synthetic','2024-01-15','Run',0,'2024-01-15T00:00:00Z'),
                ('synthetic','2024-12-30','Ride',20000,'2024-12-31T00:00:00Z');
        ''')

    def tearDown(self):
        self.c.close()

    def failures(self):
        return {check['name'] for check in check_serving_2024.recorded_week_checks(self.c)
                if check['status'] == 'fail'}

    def test_correct_metrics_pass_in_non_utc_connection(self):
        self.c.execute("SET TimeZone='America/Toronto'")
        self.assertEqual(self.failures(), set())

    def test_missing_calendar_week_fails_even_without_activities(self):
        self.c.execute("DELETE FROM data_coverage_2024 WHERE week_start_utc='2024-02-05'")
        self.assertIn('p1_complete_53_week_calendar', self.failures())

    def test_dropping_a_record_fails_reconciliation(self):
        bad_sql = f'''SELECT * REPLACE (posted_runs+1 AS posted_runs)
                      FROM ({RECORDED_WEEK_METRICS_SQL})'''
        with patch.object(check_serving_2024, 'RECORDED_WEEK_METRICS_SQL', bad_sql):
            self.assertIn('p1_record_counts_and_utc_days_reconcile', self.failures())

    def test_filling_unavailable_distance_with_zero_fails(self):
        bad_sql = f'''SELECT * REPLACE (coalesce(recorded_week_run_distance_meters,0)
                      AS recorded_week_run_distance_meters) FROM ({RECORDED_WEEK_METRICS_SQL})'''
        with patch.object(check_serving_2024, 'RECORDED_WEEK_METRICS_SQL', bad_sql):
            self.assertIn('p1_measurement_complete_distance_reconciles', self.failures())


class DeveloperSetupTests(unittest.TestCase):
    def test_explicit_fixture_database_wins_over_environment(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'fixture.duckdb'
            with duckdb.connect(str(path)) as c:
                c.execute('CREATE TABLE fixture (n INTEGER)')
            with patch.dict(os.environ, {'ENDURANCEVIZ_DB_PATH': '/does/not/exist.duckdb'}):
                self.assertEqual(ServingRepository(path).database, path.resolve())
                with self.assertRaises(FileNotFoundError):
                    ServingRepository()

    def test_setup_refuses_existing_incomplete_venv(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / '.venv').mkdir()
            with patch.object(dev, 'ROOT', root), patch.object(dev, 'VENV_PYTHON', root / '.venv/bin/python'), patch.object(dev, 'run') as run:
                with self.assertRaisesRegex(RuntimeError, 'incomplete .venv'):
                    dev.setup()
                run.assert_not_called()

    def test_setup_reuses_existing_venv_without_recreating_it(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            python = root / '.venv/bin/python'
            python.parent.mkdir(parents=True)
            python.touch()
            with patch.object(dev, 'ROOT', root), patch.object(dev, 'VENV_PYTHON', python), patch.object(dev.shutil, 'which', return_value='/tools/uv'), patch.object(dev, 'run') as run:
                dev.setup()
                self.assertEqual(run.call_count, 2)
                self.assertEqual(run.call_args_list[0].args[0][0], python)
                self.assertEqual(run.call_args_list[1].args[0], ['/tools/uv', 'pip', 'install', '--python', python, '-r', 'requirements-analysis.txt'])


if __name__ == '__main__':
    unittest.main()
