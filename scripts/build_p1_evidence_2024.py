#!/usr/bin/env python3
"""Rebuild P1 weekly evidence locally from the frozen P0 activities and source records.

Keeps the released P0 build reproducible; opt-in P1 semantics never infer successful
empty collection from legacy No Data rows. Output is ignored by Git and deployment.
"""

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from enduranceviz.analytics import (
    build_coverage, build_weekly_training, load_weekly_evidence, select_weekly_evidence,
)
from scripts.build_analytics_2024 import atomic_json, atomic_parquet, git_commit

POLICY_VERSION = 'p1-evidence-v2-draft.1'


def fingerprints(paths):
    return [
        {'path': p.relative_to(ROOT).as_posix(), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}
        for p in sorted(paths)
    ]


def build(database, output):
    if output.exists():
        raise FileExistsError('Choose a new output directory; previous builds are preserved')
    database_hash = hashlib.sha256(database.read_bytes()).hexdigest()
    code_paths = [ROOT / path for path in (
        'scripts/build_p1_evidence_2024.py', 'enduranceviz/analytics.py',
        'scripts/build_analytics_2024.py', 'scripts/build_identity_registry.py',
        'config/snapshot_2024.yaml',
    )]
    code_fingerprints = fingerprints(code_paths)
    with duckdb.connect(str(database), read_only=True) as c:
        athletes = c.execute('SELECT athlete_id::VARCHAR AS athlete_id FROM athletes').fetchdf()
        accounts = c.execute('SELECT * REPLACE(athlete_id::VARCHAR AS athlete_id) FROM athlete_external_accounts').fetchdf()
        activities = c.execute('SELECT * REPLACE(athlete_id::VARCHAR AS athlete_id) FROM activities_2024').fetchdf()
        old_coverage = c.execute('SELECT * REPLACE(athlete_id::VARCHAR AS athlete_id) FROM data_coverage_2024').fetchdf()
        old_weekly = c.execute('SELECT * REPLACE(athlete_id::VARCHAR AS athlete_id) FROM weekly_training_2024').fetchdf()
    for frame in (activities, old_coverage, old_weekly):
        frame['week_start_utc'] = pd.to_datetime(frame['week_start_utc']).dt.date
    paths = list((ROOT / 'data/raw_data').glob('*.csv')) + list((ROOT / 'data/tempdata').glob('metadata_*.csv'))
    if not paths:
        raise FileNotFoundError('Weekly source records are required for evidence reconstruction')
    source_fingerprints = fingerprints(paths)
    print('Selecting original weekly source evidence…', flush=True)
    evidence = load_weekly_evidence(paths, ROOT)
    selected = select_weekly_evidence(evidence)
    print('Checking that these sources reproduce the released P0 coverage…', flush=True)
    keys = ['athlete_id', 'week_start_utc']
    legacy = build_coverage(athletes, accounts, activities, selected)
    # DuckDB uses microseconds and the session timezone; pandas source loading
    # uses nanoseconds/UTC. Compare instants, not their storage representation.
    for frame in (old_coverage, legacy):
        frame['collection_completed_at_utc'] = pd.to_datetime(
            frame['collection_completed_at_utc'], utc=True,
        ).astype('datetime64[ns, UTC]')
    pd.testing.assert_frame_equal(
        old_coverage.set_index(keys).sort_index()[legacy.columns.drop(keys)],
        legacy.set_index(keys).sort_index(), check_dtype=False,
    )
    print('Building corrected collection states…', flush=True)
    coverage = build_coverage(athletes, accounts, activities, selected, empty_policy='unknown')
    print('Rebuilding weekly metrics with ambiguous empty values left null…', flush=True)
    # This is an evidence-layer build, not an approved cohort or annual summary.
    # Reuse the same P0 category and rolling definitions; only zero inference changes.
    import yaml
    contract = yaml.safe_load((ROOT / 'config/snapshot_2024.yaml').read_text())
    weekly = build_weekly_training(
        coverage, activities, set(contract['analytics']['strength_activity_types']),
        synthesize_observed_zeros=False,
    )
    if coverage.duplicated(keys).any() or weekly.duplicated(keys).any():
        raise ValueError('Duplicate athlete/week key')
    if set(map(tuple, old_coverage[keys].values)) != set(map(tuple, coverage[keys].values)):
        raise ValueError('Rebuild changed coverage key population')
    ambiguous = coverage['collection_error_code'].fillna('').str.contains('AMBIGUOUS_LEGACY_NO_DATA')
    if (coverage.loc[ambiguous, 'observation_status'] != 'unknown').any():
        raise ValueError('Ambiguous legacy empties still count as successful observations')
    if coverage.loc[ambiguous, 'is_complete_enough_week'].any():
        raise ValueError('Ambiguous legacy empties still count as complete weeks')
    if weekly['activity_count'].sum() != len(activities):
        raise ValueError('Canonical activity counts changed')
    no_activity = coverage.loc[~coverage['is_active_week'], keys].merge(weekly, on=keys)
    if no_activity['run_distance_meters'].notna().any():
        raise ValueError('A week without activity records acquired a numeric run distance')
    old_index = old_weekly.set_index(keys).sort_index()
    new_index = weekly.set_index(keys).sort_index()
    for field in ('activity_count', 'run_count', 'run_distance_meters', 'run_duration_seconds'):
        active = old_index['activity_count'].fillna(0) > 0
        pd.testing.assert_series_equal(old_index.loc[active, field].astype(float), new_index.loc[active, field].astype(float), check_names=False)
    joined = old_coverage.merge(coverage, on=keys, suffixes=('_old', '_new'), validate='one_to_one')
    transition = joined.groupby(['coverage_status_old', 'coverage_status_new']).size()
    if (hashlib.sha256(database.read_bytes()).hexdigest() != database_hash
            or fingerprints(paths) != source_fingerprints
            or fingerprints(code_paths) != code_fingerprints):
        raise ValueError('Inputs or implementation changed during the build; no outputs published')
    report = {
        'policy_version': POLICY_VERSION,
        'generated_at_utc': datetime.now(timezone.utc).isoformat(),
        'source_database_sha256': database_hash,
        'code_commit': git_commit(),
        'worktree_dirty': bool(subprocess.run(
            ['git', 'status', '--porcelain'], cwd=ROOT, check=True,
            capture_output=True, text=True,
        ).stdout.strip()),
        'implementation_files': code_fingerprints,
        'source_evidence_files': source_fingerprints,
        'coverage_rows': len(coverage),
        'weekly_rows': len(weekly),
        'canonical_activity_count': len(activities),
        'ambiguous_legacy_weeks': int(ambiguous.sum()),
        'ambiguous_weeks_with_activities': int((ambiguous & coverage['is_active_week']).sum()),
        'empty_weeks_now_null': len(no_activity),
        'empty_week_distances_changed_from_zero_to_null': int((
            old_index.loc[no_activity.set_index(keys).index, 'run_distance_meters'] == 0
        ).sum()),
        'old_nonnull_rolling_windows': int(old_weekly['rolling_4w_average_run_distance_meters'].notna().sum()),
        'new_nonnull_rolling_windows': int(weekly['rolling_4w_average_run_distance_meters'].notna().sum()),
        'coverage_transitions': [
            {'old': str(old), 'new': str(new), 'athlete_weeks': int(n)}
            for (old, new), n in transition.items()
        ],
        'checks': 'Original coverage reproduced from source records; unique and unchanged coverage population; activity counts and active-week metrics preserved; ambiguous observations unknown; no synthetic empty-week distances; inputs and implementation unchanged during build.',
        'limitations': 'No new cohort eligibility or annual summaries. Legacy source conflicts remain flagged. Known run-distance sums can omit missing activity measurements. Neither positive records nor successful scraping establish complete training.',
    }
    atomic_parquet(coverage, output / 'data_coverage_2024.parquet')
    atomic_parquet(weekly, output / 'weekly_training_2024.parquet')
    atomic_json(report, output / 'build.json')
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, default=ROOT / 'deploy/enduranceviz_2024.duckdb')
    parser.add_argument('--output', type=Path, default=ROOT / 'data/derived/2024/p1-evidence-v2-draft.1')
    args = parser.parse_args()
    if args.output.exists():
        parser.error('Output already exists; choose a new directory to preserve the previous build')
    report = build(args.database, args.output)
    print(json.dumps({k: v for k, v in report.items() if k != 'source_evidence_files'}, indent=2))
