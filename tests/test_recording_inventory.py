import hashlib
import unittest
from unittest.mock import patch
from bs4 import BeautifulSoup
from starlette.testclient import TestClient
from enduranceviz.serving import ServingRepository,PACKAGED_DATABASE

class RecordingInventoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import main
        cls.main=main;cls.repo=ServingRepository(PACKAGED_DATABASE);cls.client=TestClient(main.app)

    def get(self,params=None):
        with patch.object(self.main,'repository',self.repo):
            return self.client.get('/recordings',params=params)

    def test_all_recorded_athletes_accessible_without_duplicates(self):
        links=[]
        for page in range(1,11):
            response=self.get({'page':page});self.assertEqual(response.status_code,200)
            soup=BeautifulSoup(response.text,'html.parser')
            links += [a['href'] for a in soup.select('tbody a')]
        self.assertEqual(len(links),460);self.assertEqual(len(set(links)),460)
        self.assertIn('455 have Runs',self.get().text)

    def test_filters_empty_clamp_and_invalid(self):
        response=self.get({'q':'Erik Hille','event':'Marathon'})
        soup=BeautifulSoup(response.text,'html.parser')
        self.assertEqual(len(soup.select('tbody tr')),1)
        self.assertIn('179.9',soup.text)
        self.assertIn('No matching athletes',self.get({'q':'nonexistent athlete'}).text)
        self.assertIn('page 10 of 10',self.get({'page':'99999'}).text)
        for params in ({'sort':'quality'},{'event':'unknown'},{'page':'0'},{'page':'no'},{'q':'x'*201}):
            self.assertEqual(self.get(params).status_code,400)

    def test_event_assignment_matches_explained_rule(self):
        import duckdb
        with duckdb.connect(str(PACKAGED_DATABASE),read_only=True) as c:
            mismatches=c.execute('''WITH ranks AS (
                SELECT athlete_id,discipline,max(results_score) points,count(*) n,
                row_number() OVER(PARTITION BY athlete_id ORDER BY points DESC NULLS LAST,n DESC,discipline) rank
                FROM performances_2024 GROUP BY athlete_id,discipline)
                SELECT count(*) FROM ranks JOIN athlete_directory_2024 USING(athlete_id)
                WHERE rank=1 AND discipline<>primary_discipline''').fetchone()[0]
        self.assertEqual(mismatches,0)

    def test_identity_review_is_visible_without_silent_exclusion(self):
        from enduranceviz.review_notes import IDENTITY_REVIEW_NOTES
        athlete_id=next(iter(IDENTITY_REVIEW_NOTES))
        self.assertIn(athlete_id,{r['athlete_id'] for r in self.repo.comparison_athletes()})
        with patch.object(self.main,'repository',self.repo):
            response=self.client.get(f'/athlete/{athlete_id}')
            self.assertEqual(response.status_code,200)
            self.assertIn('32:12',response.text)
            comparison=self.client.get('/compare')
            self.assertIn('identity link is under review',comparison.text)
            self.assertIn(athlete_id,comparison.text)

    def test_local_audit_does_not_change_database(self):
        from scripts.audit_recorded_athletes_2024 import audit
        before=hashlib.sha256(PACKAGED_DATABASE.read_bytes()).hexdigest()
        report=audit(PACKAGED_DATABASE)
        self.assertIn('460 have stored activities',report)
        self.assertIn('IDENTITY REVIEW FIRST',report)
        self.assertIn('not an eligibility cutoff',report)
        self.assertEqual(before,hashlib.sha256(PACKAGED_DATABASE.read_bytes()).hexdigest())
