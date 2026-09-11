import importlib.util
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from enduranceviz.performance_training import PERFORMANCE_TRAINING_SQL, PAIRED_METRICS_SQL


class PerformanceTrainingTests(unittest.TestCase):
    def setUp(self):
        self.c = duckdb.connect(':memory:')
        self.addCleanup(self.c.close)
        self.c.execute('''CREATE TABLE recorded_athletes AS SELECT * FROM (VALUES
            ('a','800m','female',0.0,2.0,2,4),
            ('b','5000m','male',NULL,3.0,0,1),
            ('c','800m','female',5000.0,1.0,1,1),
            ('d','5000m','male',NULL,NULL,0,0),
            ('e','800m','unknown',NULL,NULL,0,0))
            t(athlete_id,primary_discipline,gender,median_recorded_week_run_distance_meters,
              median_recorded_week_run_count,distance_measured_weeks,recorded_run_weeks);
            CREATE TABLE performances_2024(athlete_id VARCHAR,discipline VARCHAR,results_score INTEGER);
            INSERT INTO performances_2024 VALUES
            ('a','800m',1100),('a','800m',1200),('a','800m',1200),('a','5000m',1300),
            ('b','5000m',1150),('b','5000m',NULL),('c','1500m',1250),('d','5000m',NULL);''')
        self.c.execute(f'CREATE VIEW selected AS {PERFORMANCE_TRAINING_SQL}')

    def test_one_best_score_in_primary_event_without_race_weighting(self):
        self.assertEqual(self.c.execute('''SELECT athlete_id,best_stored_results_score,
            stored_performance_count,scored_performance_count FROM selected ORDER BY 1''').fetchall(),
            [('a',1200,3,3),('b',1150,2,1),('c',None,0,0),('d',None,1,0),('e',None,0,0)])

    def test_missing_score_never_falls_back_to_other_event_or_zero(self):
        self.assertEqual(self.c.execute("SELECT best_stored_results_score FROM selected WHERE athlete_id='c'").fetchone(),(None,))

    def test_metric_denominators_and_real_zero_are_preserved(self):
        self.c.execute(f'CREATE VIEW pairs AS {PAIRED_METRICS_SQL}')
        self.assertEqual(self.c.execute('''SELECT metric,metric_value,contributing_weeks FROM pairs
            WHERE athlete_id='a' ORDER BY metric''').fetchall(),[('distance_km',0,2),('run_records',2,4)])
        self.assertEqual(self.c.execute('''SELECT metric,metric_value FROM pairs
            WHERE athlete_id='b' ORDER BY metric''').fetchall(),[('distance_km',None),('run_records',3)])

    def test_four_availability_states_partition_inventory(self):
        self.c.execute(f'CREATE VIEW pairs AS {PAIRED_METRICS_SQL}')
        self.assertEqual(self.c.execute('''SELECT
            count(*) FILTER(WHERE best_stored_results_score IS NOT NULL AND metric_value IS NOT NULL),
            count(*) FILTER(WHERE best_stored_results_score IS NULL AND metric_value IS NOT NULL),
            count(*) FILTER(WHERE best_stored_results_score IS NOT NULL AND metric_value IS NULL),
            count(*) FILTER(WHERE best_stored_results_score IS NULL AND metric_value IS NULL)
            FROM pairs WHERE metric='distance_km' ''').fetchone(),(1,1,1,2))


class PerformanceAnalysisTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name)/'fixture.duckdb'
        with duckdb.connect(str(self.database)) as c:
            c.execute('''CREATE TABLE athlete_directory_2024 AS SELECT * FROM (VALUES
                ('a','800m','female'),('b','5000m','male'),('c','800m','unknown'))
                t(athlete_id,primary_discipline,gender);
                CREATE TABLE data_coverage_2024 AS SELECT athlete_id,w::DATE AS week_start_utc,
                    w>=DATE '2024-12-30' AS is_partial_window,'unknown' AS coverage_status,
                    NULL::VARCHAR AS collection_error_code,NULL::VARCHAR AS evidence_source,
                    'unknown' AS observation_status
                    FROM athlete_directory_2024 CROSS JOIN
                    generate_series(DATE '2024-01-01',DATE '2024-12-30',INTERVAL '1 week') t(w);
                CREATE TABLE activities_2024(athlete_id VARCHAR,week_start_utc DATE,
                    activity_category VARCHAR,distance_meters DOUBLE,start_at_utc TIMESTAMPTZ);
                INSERT INTO activities_2024 VALUES
                    ('a','2024-01-01','Run',5000,'2024-01-01'),
                    ('a','2024-01-01','Run',NULL,'2024-01-01'),
                    ('a','2024-01-08','Run',0,'2024-01-08'),
                    ('a','2024-12-30','Run',99999,'2024-12-30');
                CREATE TABLE performances_2024(athlete_id VARCHAR,discipline VARCHAR,
                    gender VARCHAR,results_score INTEGER);
                INSERT INTO performances_2024 VALUES
                    ('a','800m','female',1100),('a','800m','female',1200),
                    ('a','5000m','female',1300),('b','5000m','male',1150),('c','800m','unknown',NULL);''')

    def test_report_and_local_payload_reproduce_without_mutating_database(self):
        from scripts.analyze_performance_training_2024 import analyze
        source_hash = hashlib.sha256(self.database.read_bytes()).hexdigest()
        output = Path(self.temporary.name)/'nested'/'plot.json'
        report = analyze(self.database,output)
        self.assertEqual(report,analyze(self.database))
        self.assertEqual(source_hash,hashlib.sha256(self.database.read_bytes()).hexdigest())
        payload = json.loads(output.read_text())
        self.assertEqual(payload['source_database_sha256'],source_hash)
        self.assertEqual(len(payload['athlete_summaries']),3)
        a = next(r for r in payload['athlete_summaries'] if r['recorded_sex']=='female')
        self.assertEqual((a['points'],a['distance_km'],a['run_records'],
                          a['distance_measured_weeks'],a['recorded_run_weeks']), (1200,0,1.5,1,2))
        self.assertFalse(any('athlete_id' in r or 'name' in r for r in payload['athlete_summaries']))
        self.assertIn('Other/unknown recorded-sex registry athletes: 1.',report)
        self.assertIn('| 800m | unknown | distance_km | 1 | 0 | 0 | 0 | 1 |',report)

    def test_mismatched_sex_fails_before_export(self):
        from scripts.analyze_performance_training_2024 import analyze
        with duckdb.connect(str(self.database)) as c:
            c.execute("UPDATE performances_2024 SET gender='male' WHERE athlete_id='a'")
        with self.assertRaisesRegex(ValueError,'recorded-sex'):
            analyze(self.database)

    def test_missing_athlete_coverage_cannot_silently_remove_athlete(self):
        from scripts.analyze_performance_training_2024 import analyze
        with duckdb.connect(str(self.database)) as c:
            c.execute("DELETE FROM data_coverage_2024 WHERE athlete_id='c'")
        with self.assertRaisesRegex(ValueError,'lost or duplicated'):
            analyze(self.database)


@unittest.skipUnless(importlib.util.find_spec('matplotlib'), 'Optional plotting dependencies needed')
class PerformancePlotTests(unittest.TestCase):
    def test_all_pairs_visible_empty_states_and_fixed_metric_specific_week_scale(self):
        from scripts.plot_performance_training_2024 import plot
        rows = [
            dict(event='800m',recorded_sex='female',points=1100,distance_km=0,run_records=2,
                 distance_measured_weeks=1,recorded_run_weeks=3),
            dict(event='800m',recorded_sex='male',points=1350,distance_km=250,run_records=39,
                 distance_measured_weeks=51,recorded_run_weeks=52),
            dict(event='800m',recorded_sex='male',points=None,distance_km=999,run_records=999,
                 distance_measured_weeks=1,recorded_run_weeks=1),
        ]
        seen = []
        def capture(figure,*args,**kwargs):
            axes = figure.axes[:-1]  # final axis is the colour bar
            seen.append(axes)
            self.assertEqual(sum(len(a.collections[0].get_offsets()) for a in axes if a.collections),2)
            for ax in axes:
                self.assertLess(ax.get_xlim()[0],1100)
                self.assertGreater(ax.get_xlim()[1],1350)
                self.assertEqual(ax.get_ylim()[0],0)
                self.assertGreater(ax.get_ylim()[1],250 if len(seen)==1 else 39)
                for points in ax.collections:
                    self.assertEqual((points.norm.vmin,points.norm.vmax),(1,52))
            self.assertEqual(list(axes[0].collections[0].get_array()),[1 if len(seen)==1 else 3])
            self.assertTrue(any(t.get_text()=='No paired observations' for t in axes[2].texts))
            self.assertFalse(any(a.lines for a in axes))
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp)/'inputs.json'
            source.write_text(json.dumps(dict(events=['800m','5000m'],athlete_summaries=rows)))
            with patch('matplotlib.figure.Figure.savefig',autospec=True,side_effect=capture):
                outputs = plot(source,Path(temp)/'scatter')
            self.assertEqual(len(outputs),2)
        self.assertEqual(len(seen),2)


if __name__ == '__main__':
    unittest.main()
