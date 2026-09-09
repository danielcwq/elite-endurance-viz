#!/usr/bin/env python3
"""Read-only P0 artifact validation and P1 recorded-week reconciliation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from enduranceviz.quality import validate_frames
from enduranceviz.recorded_training import RECORDED_WEEK_METRICS_SQL


# Project only fields consumed by the existing 16-check quality validator.
PROJECTIONS = {
    'athletes': ('athletes', 'athlete_id'),
    'accounts': ('athlete_external_accounts', 'provider, external_account_id, athlete_id'),
    'performances': ('performances_2024', 'athlete_id, discipline, mark_text'),
    'activities': ('activities_2024', '''activity_id, athlete_id, provider, external_account_id,
        start_at_utc, distance_meters, elapsed_seconds, moving_seconds,
        activity_category, pace_seconds_per_kilometer'''),
    'coverage': ('data_coverage_2024', 'athlete_id, week_start_utc'),
    'weekly': ('weekly_training_2024', '''athlete_id, week_start_utc, observation_status,
        activity_count, run_distance_meters'''),
    'summaries': ('athlete_summary_2024', '''total_activity_count, total_run_distance_meters,
        default_cohort_eligible'''),
}


def recorded_week_checks(connection):
    """Compare shared P1 SQL to independently grouped stored activity records."""
    # Temporary views are connection-local; the database is opened read-only.
    connection.execute(f'CREATE TEMP VIEW p1_check_weeks AS {RECORDED_WEEK_METRICS_SQL}')
    calendar_errors = connection.execute('''
        WITH calendar AS (
            SELECT athlete_id, count(*) AS n, count(DISTINCT week_start_utc) AS unique_n,
                count(*) FILTER (WHERE is_partial_window) AS partial_n,
                count(*) FILTER (WHERE week_start_utc NOT BETWEEN DATE '2024-01-01' AND DATE '2024-12-30'
                    OR isodow(week_start_utc) <> 1
                    OR is_partial_window IS DISTINCT FROM (week_start_utc = DATE '2024-12-30')) AS bad_dates
            FROM p1_check_weeks GROUP BY athlete_id
        ) SELECT count(*) FROM athletes a FULL JOIN calendar c USING (athlete_id)
          WHERE a.athlete_id IS NULL OR c.athlete_id IS NULL OR n <> 53 OR unique_n <> 53
            OR partial_n <> 1 OR bad_dates <> 0
    ''').fetchone()[0]
    count_errors, distance_errors = connection.execute('''
        WITH expected AS (
            SELECT athlete_id, week_start_utc, count(*) AS activities,
                count(*) FILTER (WHERE activity_category='Run') AS runs,
                count(DISTINCT (start_at_utc AT TIME ZONE 'UTC')::DATE)
                    FILTER (WHERE activity_category='Run') AS days,
                count(*) FILTER (WHERE activity_category='Run' AND
                    (distance_meters IS NULL OR NOT isfinite(distance_meters) OR distance_meters<0)) AS unavailable,
                sum(distance_meters) FILTER (WHERE activity_category='Run') AS distance
            FROM activities_2024 GROUP BY athlete_id, week_start_utc
        ) SELECT
            count(*) FILTER (WHERE w.athlete_id IS NULL
                OR posted_activities IS DISTINCT FROM coalesce(e.activities,0)
                OR posted_runs IS DISTINCT FROM coalesce(e.runs,0)
                OR recorded_run_days IS DISTINCT FROM coalesce(e.days,0)),
            count(*) FILTER (WHERE
                CASE WHEN coalesce(e.runs,0)=0 OR e.unavailable>0
                    THEN recorded_week_run_distance_meters IS NOT NULL
                    ELSE recorded_week_run_distance_meters IS NULL
                        OR NOT isfinite(recorded_week_run_distance_meters)
                        OR abs(recorded_week_run_distance_meters-e.distance)>0.001 END)
        FROM p1_check_weeks w FULL JOIN expected e USING (athlete_id,week_start_utc)
    ''').fetchone()
    return [
        {'name': name, 'status': 'pass' if errors == 0 else 'fail', 'detail': f'{errors} violating keys'}
        for name, errors in (
            ('p1_complete_53_week_calendar', calendar_errors),
            ('p1_record_counts_and_utc_days_reconcile', count_errors),
            ('p1_measurement_complete_distance_reconciles', distance_errors),
        )
    ]


def check_database(database: Path, contract_path: Path):
    contract = yaml.safe_load(contract_path.read_text(encoding='utf-8'))
    with duckdb.connect(str(database), read_only=True) as connection:
        frames = {name: connection.execute(f'SELECT {columns} FROM {table}').df()
                  for name, (table, columns) in PROJECTIONS.items()}
        report = validate_frames(**frames, contract=contract)
        report['checks'].extend(recorded_week_checks(connection))
    report['check_count'] = len(report['checks'])
    report['failure_count'] = sum(check['status'] == 'fail' for check in report['checks'])
    report['status'] = 'fail' if report['failure_count'] else 'pass'
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, default=ROOT / 'deploy/enduranceviz_2024.duckdb')
    parser.add_argument('--contract', type=Path, default=ROOT / 'config/snapshot_2024.yaml')
    args = parser.parse_args()
    try:
        report = check_database(args.database, args.contract)
    except (OSError, ValueError, KeyError, duckdb.Error) as exc:
        print(f'Artifact validation failed: {exc}', file=sys.stderr)
        return 1
    for check in report['checks']:
        # Keep CI output aggregate-only, without athlete rows or source payloads.
        print(f"{check['status'].upper()}: {check['name']}")
    print(f"{report['check_count']} checks; {report['failure_count']} failures")
    return int(report['status'] != 'pass')


if __name__ == '__main__':
    raise SystemExit(main())
