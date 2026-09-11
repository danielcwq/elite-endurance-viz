import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import duckdb
from bs4 import BeautifulSoup
from starlette.testclient import TestClient

import main
from enduranceviz.activity_filters import ActivityFilters
from enduranceviz.serving import ServingRepository


ATHLETE = '29430aed-6031-400f-82c0-c5ec7f7a3e7a'


class ActivityFilterTests(unittest.TestCase):
    def test_dates_are_inclusive_in_utc_even_across_leap_day(self):
        filters = ActivityFilters.parse('2024-02-29', '2024-02-29', 'Run')
        self.assertEqual(filters.utc_bounds(), (
            datetime(2024, 2, 29, tzinfo=timezone.utc),
            datetime(2024, 3, 1, tzinfo=timezone.utc)))
        self.assertEqual(ActivityFilters().utc_bounds()[1], datetime(2025, 1, 1, tzinfo=timezone.utc))

    def test_invalid_filters_are_rejected_not_silently_ignored(self):
        for start, end, category in (
            ('2023-12-31', '', 'All'), ('', '2025-01-01', 'All'),
            ('2024-03-01', '2024-02-29', 'All'), ('2024-02-30', '', 'All'),
            ('20240229', '', 'All'), ('2024-W01-1', '', 'All'),
            ('', '', "Run' OR true --"), ('', '', 'run'),
        ):
            with self.subTest(start=start, end=end, category=category), self.assertRaises(ValueError):
                ActivityFilters.parse(start, end, category)

    def test_urls_roundtrip_filters_and_reset_page(self):
        filters = ActivityFilters.parse('2024-01-01', '2024-03-31', 'Other')
        parts = urlsplit(filters.url(ATHLETE, 2))
        query = parse_qs(parts.query)
        self.assertEqual(parts.fragment, 'activities')
        self.assertEqual(query.pop('page'), ['2'])
        self.assertEqual(ActivityFilters.parse(**{k: v[0] for k, v in query.items()}), filters)
        self.assertNotIn('page=', filters.url(ATHLETE))

    def test_repository_filters_utc_boundaries_categories_and_pagination(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'fixture.duckdb'
            with duckdb.connect(str(path)) as c:
                c.execute('''CREATE TABLE activities_2024 (
                    athlete_id UUID, activity_id VARCHAR, activity_name VARCHAR, description VARCHAR,
                    provider_activity_type VARCHAR, activity_category VARCHAR, start_at_utc TIMESTAMPTZ,
                    distance_meters DOUBLE, elapsed_seconds DOUBLE, moving_seconds DOUBLE,
                    pace_seconds_per_kilometer DOUBLE, location VARCHAR, quality_status VARCHAR, quality_flags VARCHAR[])''')
                rows = [
                    ('before', 'Run', 'Run', '2024-02-28T23:59:59Z'),
                    ('first', 'Run', 'Run', '2024-02-29T00:00:00Z'),
                    ('last', 'Run', 'Run', '2024-02-29T23:59:59.999999Z'),
                    ('after', 'Run', 'Run', '2024-03-01T00:00:00Z'),
                    ('offset', 'Run', 'Run', '2024-02-28T20:00:00-05:00'),
                    ('other', 'Workout', 'Other', '2024-02-29T10:00:00Z'),
                    ('ride', 'VirtualRide', 'Ride', '2024-02-29T10:00:00Z'),
                    ('swim', 'Swim', 'Swim', '2024-02-29T10:00:00Z'),
                ]
                c.executemany('INSERT INTO activities_2024 VALUES (?, ?, ?, NULL, ?, ?, ?, 100, 10, 10, NULL, NULL, NULL, [])',
                              [[ATHLETE, activity_id, activity_id, original, category, timestamp]
                               for activity_id, original, category, timestamp in rows])
                c.execute("INSERT INTO activities_2024 SELECT '00000000-0000-0000-0000-000000000001', * EXCLUDE(athlete_id) FROM activities_2024")
            repo = ServingRepository(path)
            filters = ActivityFilters.parse('2024-02-29', '2024-02-29', 'Run')
            first = repo.activities(ATHLETE, page_size=2, filters=filters)
            self.assertEqual([r['activity_id'] for r in first['rows']], ['last', 'offset'])
            self.assertEqual((first['total'], first['total_pages'], first['first'], first['last']), (3, 2, 1, 2))
            self.assertTrue(first['has_next'])
            last = repo.activities(ATHLETE, page=10**30, page_size=2, filters=filters)
            self.assertEqual([r['activity_id'] for r in last['rows']], ['first'])
            self.assertEqual((last['page'], last['first'], last['last']), (2, 3, 3))
            self.assertFalse(last['has_next'])
            for category, original in [('Other', 'Workout'), ('Ride', 'VirtualRide'), ('Swim', 'Swim')]:
                result = repo.activities(ATHLETE, filters=ActivityFilters.parse('2024-02-29', '2024-02-29', category))
                self.assertEqual(result['total'], 1)
                self.assertEqual(result['rows'][0]['provider_activity_type'], original)
            self.assertEqual(repo.activities(ATHLETE, filters=ActivityFilters.parse('2024-02-29', '2024-02-29'))['total'], 6)
            empty = repo.activities(ATHLETE, filters=ActivityFilters.parse('2024-01-01', '2024-01-01'))
            self.assertEqual((empty['rows'], empty['total'], empty['first'], empty['last']), ([], 0, 0, 0))
            self.assertFalse(empty['has_previous'] or empty['has_next'])
            self.assertEqual(repo.activities("' OR true --", filters=filters)['total'], 0)

    def test_http_filters_preserve_chart_and_pagination_state(self):
        filters = ActivityFilters.parse('2024-01-01', '2024-03-31', 'Run')
        with TestClient(main.app) as client:
            baseline = BeautifulSoup(client.get(f'/athlete/{ATHLETE}').text, 'html.parser')
            response = client.get(filters.url(ATHLETE))
            self.assertEqual(response.status_code, 200)
            soup = BeautifulSoup(response.text, 'html.parser')
            self.assertEqual(str(soup.select_one('.training-profile')), str(baseline.select_one('.training-profile')))
            form = soup.select_one('.activity-filters')
            self.assertEqual(form['method'], 'get')
            self.assertIsNone(form.select_one('[name=page]'))
            self.assertEqual(form.select_one('[name=start]')['value'], '2024-01-01')
            self.assertEqual(form.select_one('option[selected]')['value'], 'Run')
            next_link = soup.select_one('nav.pagination a[href*="page=2"]')['href']
            self.assertEqual(next_link, filters.url(ATHLETE, 2))
            second = BeautifulSoup(client.get(next_link).text, 'html.parser')
            self.assertIn('Page 2', second.select_one('nav.pagination').text)
            self.assertEqual(second.select_one('nav.pagination a')['href'], filters.url(ATHLETE))
            first_links = {a['href'] for a in soup.select('#activities tbody a')}
            self.assertTrue(first_links.isdisjoint({a['href'] for a in second.select('#activities tbody a')}))
            bounds = filters.utc_bounds()
            with duckdb.connect(str(main.repository.database), read_only=True) as c:
                expected = c.execute('''SELECT activity_id FROM activities_2024
                    WHERE athlete_id=? AND activity_category='Run' AND start_at_utc>=? AND start_at_utc<?
                    ORDER BY start_at_utc DESC, activity_id DESC LIMIT 30''', [ATHLETE, *bounds]).fetchall()
            self.assertEqual([a['href'].rsplit('/', 1)[1] for a in soup.select('#activities tbody a')], [r[0] for r in expected])

    def test_http_invalid_and_empty_states(self):
        with TestClient(main.app) as client:
            for query in ('start=2024-02-30', 'category=Unknown', 'page=abc', 'page=0', 'start=2024-04-01&end=2024-03-01'):
                with self.subTest(query=query):
                    self.assertEqual(client.get(f'/athlete/{ATHLETE}?{query}').status_code, 400)
            response = client.get(ActivityFilters.parse('2024-01-01', '2024-01-01', 'Swim').url(ATHLETE))
            self.assertEqual(response.status_code, 200)
            soup = BeautifulSoup(response.text, 'html.parser')
            self.assertIn('No stored activities match', soup.select_one('#activities').text)
            self.assertIsNone(soup.select_one('#activities table'))
            self.assertIsNone(soup.select_one('nav.pagination'))
            self.assertEqual(len(soup.select('.training-svg')), 3)


if __name__ == '__main__':
    unittest.main()
