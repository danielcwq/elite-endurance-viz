import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from enduranceviz.recorded_training import ATHLETE_RECORDED_TRAINING_SQL
from enduranceviz.serving import ServingRepository
from scripts import build_demo_2024 as demo
from scripts.package_serving_artifact import package


class DemoDatasetTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = self.root / 'demo.duckdb'

    def test_deterministic_content_real_schema_and_metric_edge_cases(self):
        demo.build(self.database)
        other = demo.build(self.root / 'second.duckdb')
        with duckdb.connect(str(self.database),read_only=True) as c, duckdb.connect(str(other),read_only=True) as d:
            self.assertEqual(c.execute('SELECT count(*) FROM athletes').fetchone(),(5,))
            self.assertEqual(c.execute('SELECT count(*) FROM activities_2024').fetchone(),(55,))
            self.assertEqual(c.execute('SELECT count(*) FROM data_coverage_2024').fetchone(),(265,))
            self.assertEqual(c.execute('SELECT count(*) FROM duckdb_indexes()').fetchone(),(3,))
            for (table,) in c.execute("SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' ORDER BY 1").fetchall():
                # Compare relational contents, not DuckDB's incidental binary layout.
                self.assertEqual(c.execute(f'SELECT * FROM {table} ORDER BY ALL').fetchall(),
                                 d.execute(f'SELECT * FROM {table} ORDER BY ALL').fetchall())
            rows = c.execute(f'''SELECT athlete_id,recorded_run_weeks,distance_measured_weeks,
                median_recorded_week_run_count,median_recorded_week_run_days,
                median_recorded_week_run_distance_meters FROM ({ATHLETE_RECORDED_TRAINING_SQL}) ORDER BY 1''').fetchall()
            self.assertEqual(rows[0][1:],(8,8,5,5,35000))
            self.assertEqual(rows[1][1:],(2,1,1.5,1.5,0))
            self.assertEqual(rows[2][1:],(0,0,None,None,None))
            self.assertEqual(rows[3][1:],(0,0,None,None,None))
            self.assertEqual(rows[4][1:],(1,1,8,1,800))
            self.assertEqual(c.execute("SELECT count(*) FROM activities_2024 WHERE activity_id NOT LIKE 'synthetic-%'").fetchone(),(0,))
        repo = ServingRepository(self.database)
        self.assertEqual(repo.health_metadata()['dataset_version'],demo.DEMO_VERSION)
        athlete = repo.search_athletes('SYNTHETIC Frequent')[0]['athlete_id']
        self.assertEqual(len(repo.activities(athlete)['rows']),30)
        self.assertEqual(len(repo.activities(athlete,page=2)['rows']),10)
        self.assertEqual(len(repo.map_countries()),3)

    def test_no_external_inputs_read(self):
        read_bytes = Path.read_bytes
        paths = []
        def guarded(path):
            paths.append(path)
            self.assertFalse(path.is_relative_to(demo.ROOT/'data'))
            self.assertFalse(path.is_relative_to(demo.ROOT/'deploy'))
            return read_bytes(path)
        with patch.object(Path,'read_bytes',guarded):
            demo.build(self.database)
        self.assertEqual(len(paths),6)

    def test_existing_file_and_broken_symlink_are_preserved(self):
        self.database.write_bytes(b'user-owned sentinel')
        with self.assertRaises(FileExistsError):
            demo.build(self.database)
        self.assertEqual(self.database.read_bytes(),b'user-owned sentinel')
        link = self.root/'broken.duckdb'
        link.symlink_to(self.root/'missing-target')
        with self.assertRaises(FileExistsError):
            demo.build(link)
        self.assertTrue(link.is_symlink())

    def test_canonical_destinations_are_rejected(self):
        for path in (demo.ROOT/'deploy/never-create-demo.duckdb',
                     demo.ROOT/'data/derived/2024/enduranceviz_2024.duckdb'):
            with self.subTest(path=path), self.assertRaisesRegex(ValueError,'canonical or deployment'):
                demo.build(path)

    def test_failure_publishes_no_database_and_cleans_temporary_build(self):
        with patch.object(demo,'recorded_week_checks',return_value=[dict(name='fixture-failure',status='fail')]):
            with self.assertRaisesRegex(ValueError,'fixture-failure'):
                demo.build(self.database)
        self.assertEqual(list(self.root.iterdir()),[])

    def test_concurrent_destination_creation_is_not_overwritten(self):
        original_link = os.link
        def racing_link(source,destination):
            Path(destination).write_bytes(b'concurrent user data')
            original_link(source,destination)
        with patch.object(demo.os,'link',side_effect=racing_link), self.assertRaises(FileExistsError):
            demo.build(self.database)
        self.assertEqual(self.database.read_bytes(),b'concurrent user data')
        self.assertEqual(list(self.root.iterdir()),[self.database])

    def test_demo_cannot_replace_packaged_output_or_manifest(self):
        demo.build(self.database)
        destination, manifest = self.root/'packaged.duckdb', self.root/'manifest.json'
        destination.write_bytes(b'release-sentinel')
        manifest.write_text('release-manifest')
        with self.assertRaisesRegex(RuntimeError,'not be packaged'):
            package(self.database,destination,manifest)
        self.assertEqual(destination.read_bytes(),b'release-sentinel')
        self.assertEqual(manifest.read_text(),'release-manifest')

    def test_fresh_application_process_serves_only_synthetic_data(self):
        demo.build(self.database)
        before = hashlib.sha256(self.database.read_bytes()).hexdigest()
        environment = os.environ.copy()
        environment['ENDURANCEVIZ_DB_PATH'] = str(self.database)
        result = subprocess.run([sys.executable,'-c','''
import json
from starlette.testclient import TestClient
import main
c=TestClient(main.app)
assert c.get('/health').json()['dataset_version']=='SYNTHETIC-DEMO-v1'
home=c.get('/')
assert home.status_code==200 and 'Synthetic demo' in home.text
assert 'All athletes, results, and activities here are invented' in home.text
athletes=c.get('/api/athletes/search?q=SYNTHETIC').json()
assert len(athletes)==5
for athlete in athletes:
    page=c.get('/athlete/'+athlete['athlete_id'])
    assert page.status_code==200 and 'Synthetic demo' in page.text
frequent=next(a for a in athletes if a['display_name']=='SYNTHETIC Frequent')
assert c.get('/athlete/'+frequent['athlete_id']+'?page=2').status_code==200
assert c.get('/athlete/'+frequent['athlete_id']+'?start=2024-01-01&end=2024-01-07&category=Ride').status_code==200
assert c.get('/api/map/countries').status_code==200
print(json.dumps({'profiles':len(athletes),'version':main.repository.snapshot_stats()['dataset_version']}))
'''],cwd=demo.ROOT,env=environment,capture_output=True,text=True,timeout=30)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(json.loads(result.stdout),dict(profiles=5,version=demo.DEMO_VERSION))
        self.assertEqual(before,hashlib.sha256(self.database.read_bytes()).hexdigest())


if __name__ == '__main__':
    unittest.main()
