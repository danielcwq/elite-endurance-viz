import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb
from bs4 import BeautifulSoup
from fasthtml.common import to_xml
from starlette.testclient import TestClient

import main
from enduranceviz.page_metadata import metadata
from enduranceviz.serving import ServingRepository
from enduranceviz.training_profile import summarize_weeks
from scripts.build_demo_2024 import build


ROOT = Path(__file__).resolve().parents[1]


class ProfileQATests(unittest.TestCase):
    def setUp(self):
        self.repository = ServingRepository(ROOT/'deploy/enduranceviz_2024.duckdb')
        self.repo_patch = patch.object(main,'repository',self.repository)
        self.repo_patch.start()
        self.addCleanup(self.repo_patch.stop)
        self.log_patch = patch.object(logging.getLogger('enduranceviz.requests'),'disabled',True)
        self.log_patch.start()
        self.addCleanup(self.log_patch.stop)
        main.homepage.cache_clear()
        self.addCleanup(main.homepage.cache_clear)
        self.client = TestClient(main.app)
        self.addCleanup(self.client.close)

    def assert_metadata(self,soup):
        self.assertEqual(len(soup.select('head > title')),1)
        title = soup.select_one('head > title').text
        self.assertEqual(len(soup.select('head > meta[name="description"]')),1)
        description = soup.select_one('head > meta[name="description"]')['content']
        for prefix in ('og','twitter'):
            attr = 'property' if prefix=='og' else 'name'
            self.assertEqual(soup.select_one(f'head > meta[{attr}="{prefix}:title"]')['content'],title)
            self.assertEqual(soup.select_one(f'head > meta[{attr}="{prefix}:description"]')['content'],description)
        return description

    def assert_profile(self,athlete_id):
        response = self.client.get('/athlete/'+athlete_id)
        self.assertEqual(response.status_code,200)
        soup = BeautifulSoup(response.text,'html.parser')
        self.assertEqual(len(soup.select('h1')),1)
        description = self.assert_metadata(soup)
        self.assertIn('2024',soup.select_one('head > title').text)
        self.assertEqual(len(soup.select('.training-profile tbody tr')),53)
        ids = [node['id'] for node in soup.select('[id]')]
        self.assertEqual(len(ids),len(set(ids)))
        # Accessible SVG names/descriptions must resolve, not just be present.
        for svg in soup.select('.training-svg'):
            self.assertEqual(svg['role'],'img')
            for reference in svg['aria-labelledby'].split():
                target = soup.find(id=reference)
                self.assertIsNotNone(target)
                self.assertTrue(target.text.strip())
        return soup,description

    def test_all_released_coverage_states_and_missing_metadata(self):
        with duckdb.connect(str(self.repository.database),read_only=True) as c:
            cases = c.execute('''SELECT coverage_status,athlete_id::VARCHAR FROM athlete_directory_2024
                QUALIFY row_number() OVER (PARTITION BY coverage_status ORDER BY total_activity_count DESC,athlete_id)=1
                ORDER BY coverage_status''').fetchall()
            missing = c.execute('''SELECT a.athlete_id::VARCHAR FROM athletes a WHERE NOT EXISTS
                (SELECT 1 FROM performances_2024 p WHERE p.athlete_id=a.athlete_id)''').fetchall()
        self.assertEqual({status for status,_ in cases},{'high','moderate','low','unknown'})
        self.assertEqual(len(missing),1)
        for status,athlete_id in cases:
            with self.subTest(status=status):
                soup,description = self.assert_profile(athlete_id)
                summary = summarize_weeks(self.repository.recorded_weeks(athlete_id))
                self.assertIn(f"{summary['run_weeks']} of 52 full UTC weeks",description)
                self.assertIn('not a complete training history',description)
                if summary['warning_run_weeks']:
                    self.assertIn(f"{summary['warning_run_weeks']} recorded-running weeks have source warnings",description)
                    self.assertIsNotNone(soup.select_one('.training-source-note'))
                if not summary['run_weeks']:
                    self.assertFalse(soup.select('.training-svg'))
                    self.assertTrue(all(n.text=='Unavailable' for n in soup.select('.training-value')))
        for (athlete_id,) in missing:
            soup,_ = self.assert_profile(athlete_id)
            self.assertIn('No canonical 2024 performance rows.',soup.text)
            self.assertIn('No primary 2024 discipline',soup.text)

    def test_unusual_activity_types_remain_visible_with_valid_filters(self):
        with duckdb.connect(str(self.repository.database),read_only=True) as c:
            rows = c.execute('''SELECT athlete_id::VARCHAR,provider_activity_type,activity_category,
                (start_at_utc AT TIME ZONE 'UTC')::DATE AS day FROM activities_2024
                WHERE provider_activity_type IN ('WeightTraining','VirtualRide','TrailRun')
                QUALIFY row_number() OVER(PARTITION BY provider_activity_type ORDER BY start_at_utc,activity_id)=1''').fetchall()
        self.assertEqual(len(rows),3)
        for athlete_id,original,category,day in rows:
            with self.subTest(original=original):
                response = self.client.get(f'/athlete/{athlete_id}?start={day}&end={day}&category={category}')
                self.assertEqual(response.status_code,200)
                soup = BeautifulSoup(response.text,'html.parser')
                self.assertIn(original,soup.select_one('#activities').text)
                self.assert_metadata(soup)

    def test_invalid_links_are_real_404_or_400_not_empty_successes(self):
        for path in ('/athlete/not-a-uuid','/athlete/00000000-0000-0000-0000-000000000000'):
            self.assertEqual(self.client.get(path).status_code,404)
        athlete_id = self.repository.search_athletes('Jack Balick')[0]['athlete_id']
        for query in ('page=not-a-number','page=-1','start=2024-02-30','category=not-a-category'):
            self.assertEqual(self.client.get('/athlete/'+athlete_id+'?'+query).status_code,400)

    def test_homepage_title_and_share_description(self):
        soup = BeautifulSoup(self.client.get('/').text,'html.parser')
        description = self.assert_metadata(soup)
        self.assertIn('2024 snapshot',description)
        self.assertIn('not a complete or current training history',description)
        self.assertFalse(soup.select('meta[name="robots"]'))

    def test_synthetic_insufficient_profiles_and_noindex_metadata(self):
        with tempfile.TemporaryDirectory() as temporary:
            repo = ServingRepository(build(Path(temporary)/'demo.duckdb'))
            with patch.object(main,'repository',repo):
                main.homepage.cache_clear()
                soup = BeautifulSoup(self.client.get('/').text,'html.parser')
                self.assert_metadata(soup)
                self.assertIn('Synthetic Demo',soup.select_one('head > title').text)
                self.assertEqual(soup.select_one('meta[name="robots"]')['content'],'noindex, nofollow')
                profiles = repo.search_athletes('SYNTHETIC')
                self.assertTrue(any(p['coverage_status']=='insufficient' for p in profiles))
                for row in profiles:
                    soup,description = self.assert_profile(row['athlete_id'])
                    self.assertIn('invented software-test data',description)
                    self.assertEqual(soup.select_one('meta[name="robots"]')['content'],'noindex, nofollow')

    def test_metadata_escapes_markup_in_titles_and_attributes(self):
        hostile = '\"></title><script>alert(1)</script>&'
        html = ''.join(to_xml(tag) for tag in metadata(hostile,hostile))
        soup = BeautifulSoup(html,'html.parser')
        self.assertFalse(soup.select('script'))
        self.assertEqual(soup.title.text,hostile)
        self.assertEqual(soup.select_one('meta[name="description"]')['content'],hostile)


if __name__ == '__main__':
    unittest.main()
