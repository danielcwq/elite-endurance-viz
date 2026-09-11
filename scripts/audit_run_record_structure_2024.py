#!/usr/bin/env python3
"""Inspect recording granularity without merging activities or defining sessions."""

import argparse
import hashlib
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from enduranceviz.recorded_training import ATHLETE_RECORDED_TRAINING_SQL, RECORDED_WEEK_METRICS_SQL, STUDY_EVENTS
from scripts.audit_observability_2024 import table


def audit(database):
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    with duckdb.connect(str(database), read_only=True) as c:
        c.execute("SET TimeZone='UTC'")
        c.execute(f'CREATE TEMP VIEW summaries AS {ATHLETE_RECORDED_TRAINING_SQL}')
        c.execute(f'CREATE TEMP VIEW weeks AS {RECORDED_WEEK_METRICS_SQL}')
        c.execute('CREATE TEMP TABLE events(event VARCHAR, ordinal INTEGER)')
        c.executemany('INSERT INTO events VALUES (?,?)', [(event,i) for i,event in enumerate(STUDY_EVENTS)])
        c.execute('''CREATE TEMP VIEW selected AS SELECT s.*, e.ordinal FROM summaries s
            JOIN events e ON s.primary_discipline=e.event''')
        c.execute('''CREATE TEMP VIEW runs AS SELECT a.* FROM activities_2024 a JOIN selected s
            ON a.athlete_id::VARCHAR=s.athlete_id WHERE a.activity_category='Run' ''')
        c.execute('''CREATE TEMP VIEW daily AS SELECT athlete_id, start_at_utc::DATE AS day,
            count(*) AS run_records, sum(distance_meters) AS known_distance_meters,
            min(start_at_utc) AS first_start, max(start_at_utc) AS last_start
            FROM runs GROUP BY 1,2''')
        c.execute('''CREATE TEMP VIEW top_counts AS SELECT s.*, d.display_name FROM selected s
            JOIN athlete_directory_2024 d ON s.athlete_id=d.athlete_id::VARCHAR
            ORDER BY median_recorded_week_run_count DESC NULLS LAST, s.athlete_id LIMIT 3''')
        duplicate_groups = c.execute('''SELECT count(*) FROM (
            SELECT athlete_id,start_at_utc,distance_meters,moving_seconds,elapsed_seconds
            FROM activities_2024 WHERE activity_category='Run' GROUP BY ALL HAVING count(*)>1)''').fetchone()[0]
        if c.execute('''SELECT count(*) FROM weeks WHERE recorded_run_days<0 OR recorded_run_days>7
            OR recorded_run_days>posted_runs OR (posted_runs>0 AND recorded_run_days=0)''').fetchone()[0]:
            raise ValueError('Recorded run-day count violates activity/calendar bounds')
        if c.execute('SELECT sum(recorded_run_days) FROM weeks').fetchone()[0] != c.execute('''SELECT count(*) FROM (
            SELECT DISTINCT athlete_id,start_at_utc::DATE FROM activities_2024 WHERE activity_category='Run')''').fetchone()[0]:
            raise ValueError('Weekly run-day counts fail independent athlete/date reconciliation')
        lines = ['# Run records are not necessarily training sessions', '',
            f'Database SHA-256: `{before}`.', '',
            'Reproduce: `.venv/bin/python scripts/audit_run_record_structure_2024.py`.', '',
            '## Why this check was needed', '',
            'The approved exploratory frequency measure counts stored Run records. Distinct records may be '
            'warm-ups, repetitions, recoveries, cool-downs, separate sessions, or overlapping recordings. '
            'Unique activity IDs alone do not establish independent training sessions.', '',
            f'Across the entire snapshot there are {duplicate_groups} repeated Run fingerprint groups using '
            '(athlete, exact start timestamp, distance, moving duration, elapsed duration). This checks exact '
            'record repetition, not near-duplicates or overlapping activities. No records were merged or removed.', '',
            '## Three highest athlete median record counts', '',
            'Selected deterministically across the six study events by median recorded-week Run count, '
            'then athlete ID. This is an outlier inspection, not an athlete exclusion rule. '
            'Record length and maximum daily counts below use all stored 2024 runs; weekly medians use full weeks.', '']
        lines += table(c, '''WITH lengths AS (SELECT athlete_id, median(distance_meters) AS median_record_meters,
            median(moving_seconds) AS median_moving_seconds FROM runs GROUP BY 1),
            days AS (SELECT athlete_id, max(run_records) AS maximum_daily_records FROM daily GROUP BY 1)
            SELECT t.display_name, t.primary_discipline AS event, t.recorded_run_weeks,
                t.median_recorded_week_run_count AS median_records_per_recorded_week,
                t.median_recorded_week_run_days AS median_recorded_running_days,
                round(l.median_record_meters,1) AS median_record_meters,
                l.median_moving_seconds, d.maximum_daily_records
            FROM top_counts t JOIN lengths l ON t.athlete_id=l.athlete_id::VARCHAR
            JOIN days d ON t.athlete_id=d.athlete_id::VARCHAR
            ORDER BY median_records_per_recorded_week DESC, t.display_name''')
        lines += ['## Concrete trace: Ruken Tek, January 5', '',
            'This date was inspected after the high-count outlier appeared. The ordered source records below '
            'are consistent with separately recorded repetitions; they do not prove a particular workout structure.', '']
        lines += table(c, '''SELECT d.day, d.run_records,
            round(d.known_distance_meters/1000,3) AS known_distance_km, d.first_start, d.last_start
            FROM daily d JOIN athlete_directory_2024 a USING(athlete_id)
            WHERE a.display_name='Ruken Tek' AND d.day=DATE '2024-01-05' ''')
        lines += table(c, '''SELECT r.activity_id, r.start_at_utc, round(r.distance_meters,2) AS distance_meters,
            r.moving_seconds, r.elapsed_seconds, r.source_file, r.source_row_number
            FROM runs r JOIN athlete_directory_2024 d USING(athlete_id)
            WHERE d.display_name='Ruken Tek' AND r.start_at_utc::DATE=DATE '2024-01-05'
            ORDER BY r.start_at_utc, r.activity_id LIMIT 8''')
        lines += ['## Recorded running days as a separate diagnostic', '',
            'A recorded running day is a UTC date with at least one stored Run. It is unaffected by splitting '
            'one day into many records, but cannot distinguish single from double sessions or recover missing training. '
            'Within each athlete, take the median over weeks with recorded runs, then the median across athletes. '
            'This supports interpretation of the approved record-count measure; it does not replace it.', '']
        lines += table(c, '''SELECT primary_discipline AS event, gender AS recorded_sex,
            count(median_recorded_week_run_count) AS athletes,
            median(median_recorded_week_run_count) AS median_run_records,
            median(median_recorded_week_run_days) AS median_recorded_running_days
            FROM selected GROUP BY ordinal,1,2 ORDER BY ordinal,2''')
        lines += ['## Decision boundary', '',
            '- Keep the original metric labeled Run record count, not training-session count. The approved values are unchanged.',
            '- Recorded running days can accompany record counts without inventing a session boundary.',
            '- Defining sessions requires an explicit protocol for elapsed-time overlaps, gaps, missing duration, '
            'and separately recorded warm-up/repetition/cool-down activity. No gap threshold or session grouping has been selected.',
            '- The inspected pattern suggests record fragmentation. It does not justify excluding an athlete or dropping short runs.',
            '- Distance sums still need overlap/near-duplicate checks; an exact-fingerprint check is not sufficient validation of every sum.', '',
            'Validation: recorded run-day counts are bounded by calendar days and Run records, and their total '
            'reconciles to independent distinct athlete/UTC-date counts. Production is unchanged.', '']
    if hashlib.sha256(database.read_bytes()).hexdigest() != before:
        raise ValueError('Source database changed')
    return '\n'.join(lines)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, default=ROOT/'deploy/enduranceviz_2024.duckdb')
    parser.add_argument('--output', type=Path, default=ROOT/'docs/p1-run-record-structure-2024.md')
    args = parser.parse_args()
    args.output.write_text(audit(args.database), encoding='utf-8')
    print(f'Wrote {args.output}')
