import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from enduranceviz.serving import ServingRepository
from scripts.build_demo_2024 import build
from scripts import benchmark_activity_serving as bench


class ActivityConnectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = build(Path(self.temp.name)/'synthetic.duckdb')
        self.repository = ServingRepository(self.database)
        self.athlete = self.repository.search_athletes('SYNTHETIC Frequent')[0]['athlete_id']

    def test_count_and_rows_use_one_readonly_connection_then_close(self):
        original = duckdb.connect
        connections = []
        def connect(*args,**kwargs):
            self.assertEqual(kwargs,{'read_only':True})
            connection = original(*args,**kwargs)
            connections.append(connection)
            return connection
        with patch('enduranceviz.serving.duckdb.connect',side_effect=connect):
            page = self.repository.activities(self.athlete,page=1000000)
        self.assertEqual((page['total'],page['page'],len(page['rows'])),(40,2,10))
        self.assertEqual(len(connections),1)
        with self.assertRaises(duckdb.ConnectionException):
            connections[0].execute('SELECT 1')

    def test_connection_closes_when_query_fails(self):
        original = duckdb.connect
        connections = []
        def connect(*args,**kwargs):
            connection = original(*args,**kwargs)
            connections.append(connection)
            return connection
        with patch('enduranceviz.serving.duckdb.connect',side_effect=connect), \
             patch.object(self.repository,'_fetch',side_effect=ValueError('fixture query failure')):
            with self.assertRaisesRegex(ValueError,'fixture query failure'):
                self.repository.activities(self.athlete)
        self.assertEqual(len(connections),1)
        with self.assertRaises(duckdb.ConnectionException):
            connections[0].execute('SELECT 1')

    def test_benchmark_end_to_end_exports_aggregates_not_ids_or_html(self):
        with patch.object(bench,'load_baseline',return_value=(ServingRepository,'fixture-code-hash')):
            payload = bench.benchmark(self.database,iterations=5,warmups=1)
        self.assertEqual(len(payload['results']),5)
        self.assertEqual([r['history_records'] for r in payload['results']],[8,40,40,40,0])
        self.assertEqual([r['returned_rows'] for r in payload['results']],[8,30,10,0,0])
        for result in payload['results']:
            self.assertTrue(result['equivalent_results_and_html'])
            self.assertGreater(result['before_html_bytes'],0)
            self.assertEqual(result['before_html_bytes'],result['after_html_bytes'])
        exported = json.dumps(payload)
        self.assertNotIn(self.athlete,exported)
        self.assertNotIn('SYNTHETIC Frequent',exported)
        self.assertNotIn('<html',exported)
        self.assertIn('not browser-load',bench.report(payload))


class BenchmarkProtocolTests(unittest.TestCase):
    def test_pair_order_alternates_and_warmups_are_not_counted(self):
        calls = []
        def before():
            calls.append('before')
            return {'same':True}
        def after():
            calls.append('after')
            return {'same':True}
        samples,value = bench.paired_measure(before,after,iterations=3,warmups=1)
        self.assertEqual(calls,['before','after','after','before','before','after','after','before'])
        self.assertEqual([len(v) for v in samples.values()],[3,3])
        self.assertEqual(value,{'same':True})

    def test_mismatch_stops_measurement(self):
        with self.assertRaisesRegex(ValueError,'results differ'):
            bench.paired_measure(lambda:1,lambda:2,iterations=5,warmups=1)

    def test_protocol_limits_and_baseline_identifier(self):
        for iterations,warmups in ((0,1),(101,1),(5,0),(5,11)):
            with self.assertRaises(ValueError):
                bench.benchmark(Path('unused'),iterations,warmups)
        with self.assertRaisesRegex(ValueError,'full local Git commit SHA'):
            bench.load_baseline('HEAD:arbitrary.py')
        self.assertEqual(bench.summarize(list(range(1,21)))['p95_ms'],19)


if __name__ == '__main__':
    unittest.main()
