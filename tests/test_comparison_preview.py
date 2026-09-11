import hashlib
import unittest
from unittest.mock import patch

from bs4 import BeautifulSoup
from starlette.testclient import TestClient

from enduranceviz.comparison_preview import overlay_chart, metric_rows, quantile
from enduranceviz.serving import ServingRepository, PACKAGED_DATABASE


class ComparisonPreviewTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import main
        cls.main = main
        cls.repository = ServingRepository(PACKAGED_DATABASE)
        cls.client = TestClient(main.app)

    def get(self, params=None):
        with patch.object(self.main, 'repository', self.repository):
            return self.client.get('/compare', params=params)

    def test_default_contributors_labels_and_unique_ids(self):
        response = self.get()
        self.assertEqual(response.status_code, 200)
        soup = BeautifulSoup(response.text, 'html.parser')
        self.assertEqual(len(soup.select('.comparison-chart .dot')), 138)
        self.assertEqual(len(soup.select('h1')), 1)
        self.assertEqual(len(soup.select('.comparison-chart')), 1)
        self.assertEqual(len(soup.select('.cohort-button')), 4)
        self.assertEqual([b['data-series'] for b in soup.select('.cohort-button')],['0','1','2','3'])
        self.assertEqual(len(soup.select('button[data-point]')),138)
        for count in ('22 contributors', '62 contributors', '15 contributors', '39 contributors'):
            self.assertIn(count, response.text)
        ids = [e['id'] for e in soup.select('[id]')]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertTrue(all(soup.find(id=label) for s in soup.select('svg[aria-labelledby]') for label in s['aria-labelledby'].split()))
        self.assertEqual(len(soup.select('[role="region"][tabindex="0"][aria-label]')), 3)
        self.assertEqual(soup.select_one('form')['method'], 'get')

    def test_all_event_metric_view_combinations_and_same_event(self):
        from enduranceviz.recorded_training import PREVIEW_EVENTS
        for event in PREVIEW_EVENTS:
            for metric in ('distance', 'records'):
                for view in ('distribution', 'points'):
                    with self.subTest(event=event, metric=metric, view=view):
                        response = self.get(dict(first=event, second=event, metric=metric, view=view))
                        soup = BeautifulSoup(response.text, 'html.parser')
                        self.assertEqual(response.status_code, 200)
                        self.assertEqual(len(soup.select('.series')), 2)
                        self.assertEqual(soup.select_one('select[name="view"] option[selected]')['value'], view)
                        self.assertEqual(soup.select_one('select[name="metric"] option[selected]')['value'], metric)
                        import math
                        self.assertTrue(all(math.isfinite(float(p[attr])) for p in soup.select('circle') for attr in ('cx','cy')))

    def test_invalid_filters(self):
        for params in ({'first':'marathon'}, {'metric':'pace'}, {'view':'regression'}, {'second':"' OR 1=1"}):
            self.assertEqual(self.get(params).status_code, 400)

    def test_empty_panels_and_synthetic_identification(self):
        stats = dict(self.repository.snapshot_stats(), dataset_version='SYNTHETIC-DEMO-v1')
        with patch.object(self.repository, 'comparison_athletes', return_value=()), \
             patch.object(self.repository, 'snapshot_stats', return_value=stats):
            response = self.get({'view':'points'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text.count('No contributing observations'), 1)
        self.assertIn('Synthetic demo — not real athletes', response.text)
        soup = BeautifulSoup(response.text, 'html.parser')
        self.assertEqual(soup.select_one('meta[name="robots"]')['content'], 'noindex, nofollow')
        self.assertFalse(soup.select('.dot'))

    def test_database_unchanged_and_unique_athletes(self):
        before = hashlib.sha256(PACKAGED_DATABASE.read_bytes()).hexdigest()
        rows = self.repository.comparison_athletes()
        self.assertEqual(len(rows), 3412)
        self.assertEqual(len({r['athlete_id'] for r in rows}), 3412)
        self.assertEqual(len(metric_rows(rows, 'distance', 'points')), 450)
        self.assertEqual(before, hashlib.sha256(PACKAGED_DATABASE.read_bytes()).hexdigest())

    def test_matches_reproducible_static_plot_inputs(self):
        from scripts.analyze_performance_training_2024 import prepare
        import duckdb
        with duckdb.connect(str(PACKAGED_DATABASE), read_only=True) as c:
            prepare(c)
            expected = c.execute('''SELECT athlete_id, median_recorded_week_run_distance_meters,
                median_recorded_week_run_count, distance_measured_weeks, recorded_run_weeks,
                best_stored_results_score FROM selected ORDER BY athlete_id''').fetchall()
        from enduranceviz.recorded_training import STUDY_EVENTS
        actual = sorted((r['athlete_id'],r['median_recorded_week_run_distance_meters'],
            r['median_recorded_week_run_count'],r['distance_measured_weeks'],r['recorded_run_weeks'],
            r['best_stored_results_score']) for r in self.repository.comparison_athletes() if r['primary_discipline'] in STUDY_EVENTS)
        self.assertEqual(actual, expected)

    def test_missing_zero_and_missing_score(self):
        rows = [dict(name='<script>bad</script>',athlete_id='a', median_recorded_week_run_distance_meters=0,
                     median_recorded_week_run_count=2, distance_measured_weeks=1,recorded_run_weeks=4,
                     source_warning_run_weeks=3,best_stored_results_score=None)]
        self.assertEqual(metric_rows(rows,'distance','distribution')[0]['value'],0)
        self.assertEqual(metric_rows(rows,'distance','distribution')[0]['weeks'],1)
        self.assertEqual(metric_rows(rows,'records','distribution')[0]['weeks'],4)
        self.assertEqual(metric_rows(rows,'distance','points'),[])
        html = str(overlay_chart([dict(id=0,sex='female',label='test',rows=metric_rows(rows,'distance','distribution'))],'distribution','km'))
        self.assertIn('&lt;script&gt;',html)
        self.assertNotIn('<script>',html)
        self.assertIn('No contributing observations',str(overlay_chart([],'points','km')))
        rows[0]['median_recorded_week_run_distance_meters'] = None
        self.assertEqual(metric_rows(rows,'distance','distribution'),[])

    def test_quantile_matches_static_plot_method(self):
        import numpy as np
        for values in ([1],[1,3],[0,2,8,11,20]):
            for fraction in (.25,.5,.75):
                self.assertEqual(quantile(values,fraction),np.quantile(values,fraction))

    def test_sex_selection_never_pools_and_marathon_available(self):
        for sex,expected in [('female',29),('male',60),('both',89)]:
            response=self.get(dict(first='Marathon',second='Marathon',sex=sex,view='points'))
            soup=BeautifulSoup(response.text,'html.parser')
            self.assertEqual(len(soup.select('.dot')),expected)
            self.assertEqual(len(soup.select('.series')),2 if sex=='both' else 1)
            self.assertEqual(soup.select_one('select[name="sex"] option[selected]')['value'],sex)
        self.assertEqual(self.get({'sex':'pooled'}).status_code,400)

    def test_empirical_curve_ties_and_coordinates(self):
        import re
        rows=[dict(athlete_id=str(i),name='test',value=value,weeks=1,
                   source_warning_run_weeks=0,best_stored_results_score=1100) for i,value in enumerate([0,5,5,10])]
        html=str(overlay_chart([dict(id=0,sex='female',label='test',rows=rows)],'distribution','km'))
        soup=BeautifulSoup(html,'html.parser')
        self.assertEqual([float(p['cy']) for p in soup.select('.dot')],[282,126,126,48])
        self.assertEqual([float(p['cx']) for p in soup.select('.dot')],[72,231.6,231.6,391.2])
        self.assertTrue(soup.select_one('.curve')['d'].endswith('H870'))
        self.assertIn('no smoothing',self.get().text)
