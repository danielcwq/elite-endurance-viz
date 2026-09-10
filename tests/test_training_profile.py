import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from xml.etree import ElementTree

import duckdb
from bs4 import BeautifulSoup
from fasthtml.common import to_xml
from starlette.testclient import TestClient

import main
from enduranceviz.recorded_training import ATHLETE_RECORDED_TRAINING_SQL
from enduranceviz.serving import ServingRepository
from enduranceviz.training_profile import summarize_weeks, timeline_svg, training_section


def week(index, **changes):
    return {'week_start_utc':date(2024,1,1)+timedelta(weeks=index),
            'is_partial_window':False, 'posted_runs':0, 'posted_activities':0,
            'recorded_run_days':0, 'recorded_week_run_distance_meters':None,
            'evidence_state':'ambiguous_empty_record', 'collection_error_code':None,
            'runs_missing_distance':0, 'runs_invalid_distance':0, **changes}


class TrainingProfileTests(unittest.TestCase):
    def test_summary_preserves_missing_zero_and_partial_week_distinctions(self):
        rows = [week(0), week(1,posted_runs=1,recorded_run_days=1,recorded_week_run_distance_meters=0),
                week(2,posted_runs=2,recorded_run_days=2,recorded_week_run_distance_meters=10000,evidence_state='source_warning'),
                week(3,posted_runs=3,recorded_run_days=1,runs_missing_distance=1),
                week(52,posted_runs=9,recorded_run_days=2,recorded_week_run_distance_meters=100000,is_partial_window=True)]
        result=summarize_weeks(rows)
        self.assertEqual((result['run_weeks'],result['distance_weeks'],result['warning_run_weeks']),(3,2,1))
        self.assertEqual((result['distance_median'],result['record_median'],result['day_median']),(5000,2,1))

    def test_svg_has_gaps_real_zero_and_warning_without_interpolation(self):
        rows=[week(0),week(1,posted_runs=1,recorded_week_run_distance_meters=0),
              week(2,posted_runs=1,recorded_week_run_distance_meters=10000,evidence_state='source_warning')]
        root=ElementTree.fromstring(str(timeline_svg(rows,'recorded_week_run_distance_meters','Distance',1000)))
        self.assertEqual(len(root.findall('.//rect[@class="training-gap"]')),1)
        self.assertEqual(len(root.findall('.//circle[@class="training-zero"]')),1)
        self.assertEqual(len(root.findall('.//rect[@class="training-bar warning"]')),1)
        self.assertEqual(root.attrib['role'],'img')

    def test_empty_profile_does_not_display_training_zero(self):
        html=to_xml(training_section([week(i,is_partial_window=i==52) for i in range(53)]))
        self.assertIn('No Run records are available',html)
        self.assertNotIn('<svg',html)
        soup=BeautifulSoup(html,'html.parser')
        self.assertTrue(all(p.text=='Unavailable' for p in soup.select('.training-value')))
        self.assertEqual(len(soup.select('tbody tr')),53)
        evidence = soup.select_one('.training-table-scroll')
        self.assertEqual(evidence['tabindex'],'0')
        self.assertEqual(evidence['role'],'region')
        self.assertIn('Weekly recorded running and source evidence',evidence['aria-label'])

    def test_source_errors_are_escaped_in_table(self):
        html=to_xml(training_section([week(0,collection_error_code='<script>alert(1)</script>')]))
        self.assertNotIn('<script>',html)
        self.assertIn('&lt;script&gt;',html)

    def test_timestamp_labels_convert_to_utc(self):
        self.assertEqual(main.format_timestamp(datetime(2024,1,1,23,30,tzinfo=timezone(timedelta(hours=-5)))),
                         '2024-01-02 04:30 UTC')
        self.assertEqual(main.format_timestamp('2024-01-02T04:30:00Z'),'2024-01-02 04:30 UTC')

    def test_scoped_repository_matches_exploratory_sql(self):
        database=Path(__file__).resolve().parents[1]/'deploy/enduranceviz_2024.duckdb'
        repository=ServingRepository(database)
        with duckdb.connect(str(repository.database),read_only=True) as c:
            rows=c.execute(f'''SELECT athlete_id, median_recorded_week_run_distance_meters,
                median_recorded_week_run_count, median_recorded_week_run_days
                FROM ({ATHLETE_RECORDED_TRAINING_SQL})
                WHERE athlete_id IN ('0d6ef2f6-da6b-4494-b439-89b2bb43f7a3','29430aed-6031-400f-82c0-c5ec7f7a3e7a')''').fetchall()
        self.assertEqual(len(rows),2)
        for athlete_id,distance,records,days in rows:
            weeks=repository.recorded_weeks(athlete_id)
            self.assertEqual(len(weeks),53)
            self.assertEqual({str(w['athlete_id']) for w in weeks},{athlete_id})
            summary=summarize_weeks(weeks)
            self.assertAlmostEqual(summary['distance_median'],distance)
            self.assertEqual((summary['record_median'],summary['day_median']),(records,days))
        self.assertEqual(repository.recorded_weeks("' OR true --"),())

    def test_http_profile_exposes_new_metrics_and_accessible_weekly_data(self):
        with TestClient(main.app) as client:
            response=client.get('/athlete/0d6ef2f6-da6b-4494-b439-89b2bb43f7a3')
        self.assertEqual(response.status_code,200)
        soup=BeautifulSoup(response.text,'html.parser')
        self.assertEqual(len(soup.select('h1')),1)
        self.assertEqual(len(soup.select('.training-svg')),3)
        self.assertEqual(len(soup.select('.training-profile tbody tr')),53)
        self.assertIn('39.0',soup.select_one('.training-stats').text)
        self.assertNotIn('per 366/7 calendar week',response.text)
        self.assertNotIn('coverage (',response.text)

    def test_profile_scroll_regions_have_explicit_keyboard_and_accessible_contract(self):
        with TestClient(main.app) as client:
            response=client.get('/athlete/0d6ef2f6-da6b-4494-b439-89b2bb43f7a3')
        soup=BeautifulSoup(response.text,'html.parser')
        regions=soup.select('.training-chart-scroll,.training-table-scroll,.table-wrap')
        self.assertEqual(len(regions),5)
        labels=[]
        for region in regions:
            self.assertEqual(region['tabindex'],'0')
            self.assertEqual(region['role'],'region')
            self.assertTrue(region['aria-label'].strip())
            labels.append(region['aria-label'])
        self.assertEqual(len(set(labels)),5)


if __name__ == '__main__':
    unittest.main()
