#!/usr/bin/env python3
"""Build an isolated, entirely synthetic serving database without reading athlete data."""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from enduranceviz.analytics import build_weekly_training, build_athlete_summaries, reconciliation_report
from enduranceviz.database import SERVING_OBJECTS_SQL
from scripts.check_serving_2024 import recorded_week_checks

DEMO_VERSION = 'SYNTHETIC-DEMO-v1'
DEMO_DATABASE = ROOT / 'data/derived/2024/demo/synthetic.duckdb'
FIXED_TIME = datetime(2025, 1, 1, tzinfo=timezone.utc)


def build(database=DEMO_DATABASE):
    database = Path(database).absolute()
    protected = ROOT / 'deploy'
    if database.resolve().is_relative_to(protected.resolve()) or database.resolve() == (
        ROOT / 'data/derived/2024/enduranceviz_2024.duckdb'
    ).resolve():
        raise ValueError('Demo output cannot target a canonical or deployment database')
    if os.path.lexists(database):
        raise FileExistsError(f'Refusing to overwrite existing demo destination: {database}')
    inputs = [ROOT / p for p in (
        'schema/2024.sql', 'fixtures/synthetic_2024.sql', 'config/snapshot_2024.yaml',
        'enduranceviz/analytics.py', 'enduranceviz/database.py', 'scripts/build_demo_2024.py',
    )]
    contents = {path: path.read_bytes() for path in inputs}
    fingerprint = hashlib.sha256(b''.join(contents.values())).hexdigest()
    build_id = str(uuid.uuid5(uuid.NAMESPACE_URL, 'enduranceviz-synthetic:' + fingerprint))
    contract = yaml.safe_load(contents[ROOT / 'config/snapshot_2024.yaml'])
    contract['dataset']['specification_version'] = DEMO_VERSION
    database.parent.mkdir(parents=True, exist_ok=True)
    # Same-filesystem temporary directory + exclusive hard-link publication. A
    # destination created concurrently is never replaced; failed builds publish nothing.
    with tempfile.TemporaryDirectory(prefix='.synthetic-build-', dir=database.parent) as temporary:
        staged = Path(temporary) / 'synthetic.duckdb'
        with duckdb.connect(str(staged)) as c:
            c.execute(contents[ROOT / 'schema/2024.sql'].decode())
            c.execute(contents[ROOT / 'fixtures/synthetic_2024.sql'].decode())
            c.execute("INSERT INTO dataset_builds VALUES (?, 'enduranceviz-2024', ?, ?, ?, ?, 'succeeded', ?)",
                      [build_id, DEMO_VERSION, 'synthetic-generator-sha256:' + fingerprint,
                       FIXED_TIME, FIXED_TIME, 'synthetic-demo-only'])
            for path, content in contents.items():
                relative = path.relative_to(ROOT).as_posix()
                digest = hashlib.sha256(content).hexdigest()
                c.execute('INSERT INTO import_manifest VALUES (?,?,?,?,?,?,?,?,?,?)',
                          [hashlib.sha256(relative.encode()).hexdigest(), build_id, relative,
                           'synthetic', 'generator-definition', digest, len(content), 0,
                           hashlib.sha256(b'code-or-configuration-not-tabular').hexdigest(), FIXED_TIME])
            athletes = c.execute('SELECT athlete_id::VARCHAR AS athlete_id FROM athletes').df()
            coverage = c.execute('SELECT * REPLACE(athlete_id::VARCHAR AS athlete_id) FROM data_coverage_2024').df()
            activities = c.execute('SELECT * REPLACE(athlete_id::VARCHAR AS athlete_id) FROM activities_2024').df()
            for frame in (coverage, activities):
                frame['week_start_utc'] = frame['week_start_utc'].dt.date
            # Exercise released P0 storage functions; P1 serving computes its
            # measurement-aware metrics on read, exactly as with the real snapshot.
            weekly = build_weekly_training(coverage, activities, set(contract['analytics']['strength_activity_types']))
            summaries = build_athlete_summaries(athletes, coverage, weekly, activities, contract, FIXED_TIME)
            reconciliation = reconciliation_report(activities, weekly, summaries)
            if not reconciliation['activity_count_reconciles'] or reconciliation['run_distance_max_absolute_difference_meters'] > .001:
                raise ValueError('Synthetic activity/weekly/summary reconciliation failed')
            for table, frame in (('weekly_training_2024', weekly), ('athlete_summary_2024', summaries)):
                c.register('synthetic_input', frame)
                c.execute(f'INSERT INTO {table} BY NAME SELECT * FROM synthetic_input')
                c.unregister('synthetic_input')
            c.execute(SERVING_OBJECTS_SQL)
            failures = [check['name'] for check in recorded_week_checks(c) if check['status'] != 'pass']
            if failures:
                raise ValueError(f'Synthetic P1 metric checks failed: {failures}')
            c.execute('CHECKPOINT')
        os.link(staged, database)
    return database


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, default=DEMO_DATABASE)
    args = parser.parse_args()
    try:
        output = build(args.database)
    except (OSError, ValueError, duckdb.Error) as exc:
        parser.exit(1, f'error: {exc}\n')
    print(f'Built {DEMO_VERSION}: {output} (invented data; not for production)')
