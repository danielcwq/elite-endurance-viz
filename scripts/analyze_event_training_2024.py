#!/usr/bin/env python3
"""Exploratory distributions of athlete summaries of recorded running weeks."""

import argparse
import hashlib
import json
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from enduranceviz.recorded_training import (
    ATHLETE_RECORDED_TRAINING_SQL, RECORDED_WEEK_METRICS_SQL, POLICY_VERSION, STUDY_EVENTS,
)
from scripts.audit_observability_2024 import table


def analyze(database, figure_data=None):
    source_hash = hashlib.sha256(database.read_bytes()).hexdigest()
    with duckdb.connect(str(database), read_only=True) as c:
        c.execute(f'CREATE TEMP VIEW recorded_weeks AS {RECORDED_WEEK_METRICS_SQL}')
        c.execute(f'CREATE TEMP VIEW recorded_athletes AS {ATHLETE_RECORDED_TRAINING_SQL}')
        c.execute('CREATE TEMP TABLE event_order(event VARCHAR, ordinal INTEGER)')
        c.executemany('INSERT INTO event_order VALUES (?,?)', [(event, i) for i, event in enumerate(STUDY_EVENTS)])
        c.execute('CREATE TEMP VIEW selected AS SELECT a.*, e.ordinal FROM recorded_athletes a JOIN event_order e ON a.primary_discipline=e.event')
        if c.execute('SELECT count(*)-count(DISTINCT athlete_id) FROM recorded_athletes').fetchone()[0]:
            raise ValueError('Duplicate athlete summaries')
        if c.execute('SELECT count(*) FROM recorded_athletes').fetchone()[0] != c.execute('SELECT count(*) FROM athlete_directory_2024').fetchone()[0]:
            raise ValueError('Directory athletes lost from inventory')
        actual = c.execute("SELECT count(*) FROM activities_2024 WHERE activity_category='Run'").fetchone()[0]
        if c.execute('SELECT sum(recorded_runs_2024) FROM recorded_athletes').fetchone()[0] != actual:
            raise ValueError('Recorded run counts do not reconcile')
        if c.execute('SELECT count(*) FROM recorded_weeks WHERE (posted_runs=0 OR runs_missing_distance>0 OR runs_invalid_distance>0) AND recorded_week_run_distance_meters IS NOT NULL').fetchone()[0]:
            raise ValueError('Unavailable distance has become numeric')
        lines = ['# Exploratory recorded-week training comparisons', '',
            f'Calculation policy: `{POLICY_VERSION}`. Source database SHA-256: `{source_hash}`.', '',
            'Reproduce: `.venv/bin/python scripts/analyze_event_training_2024.py`.', '',
            'For the local figure, install `requirements-analysis.txt`, then run:', '',
            '```sh',
            '.venv/bin/python scripts/analyze_event_training_2024.py --figure-data data/derived/2024/p1-exploration/training-figure.json',
            '.venv/bin/python scripts/plot_event_training_2024.py data/derived/2024/p1-exploration/training-figure.json data/derived/2024/p1-exploration/training-distributions.png',
            '```', '',
            'Figure inputs and PNG are local, Git-ignored outputs; no new athlete-level data is published.', '',
            '## Question and interpretation', '',
            'How do publicly observed running distance and run frequency vary across event specializations? '
            '800m versus 5000m leads; 1500m provides context, 10000m extends the track comparison, '
            'steeplechase remains separate, and half marathon is an exploratory extension.', '',
            'These tables describe **weeks with recorded runs**, not all training weeks or a complete year. '
            'Each contributing athlete supplies one median. Event/recorded-sex cells then summarize those athlete medians. '
            'This exploratory view was approved by Daniel in [P1 analysis decisions](p1-analysis-decisions.md); '
            'it is not a finalized inclusion protocol or a confirmatory test.', '',
            'No annual-week or P0 coverage cutoff is applied. Full weeks are Monday–Sunday UTC; December 30–31 '
            'is retained in annual record counts only. Source-warning weeks retain their actual activities. '
            'Weekly distance is unavailable when any recorded run lacks a finite, nonnegative distance. '
            'Run counts remain usable as record counts. An athlete with no recorded runs remains in the inventory '
            'but contributes no running-week summary.', '', '## Contributors and measurement availability', '']
        lines += table(c, """SELECT primary_discipline AS event, gender AS recorded_sex, count(*) AS registry,
            count(*) FILTER (WHERE recorded_runs_2024>0) AS any_run_2024,
            count(median_recorded_week_run_count) AS frequency_contributors,
            count(median_recorded_week_run_distance_meters) AS distance_contributors,
            sum(recorded_run_weeks) AS recorded_run_weeks,
            sum(distance_unavailable_run_weeks) AS distance_unavailable_weeks,
            sum(source_warning_run_weeks) AS source_warning_run_weeks
            FROM selected GROUP BY ordinal,1,2 ORDER BY ordinal,2""")
        lines += ['## Distribution of athlete recorded-week medians', '',
            'Q1 and Q3 are continuous 25th/75th percentiles across athlete medians, **not confidence intervals**. '
            'Distance and frequency summarize their own contributing weeks. Small cells are shown descriptively, '
            'without significance tests or generalization to the event population.', '']
        lines += table(c, """SELECT primary_discipline AS event, gender AS recorded_sex,
            round(quantile_cont(median_recorded_week_run_distance_meters/1000,0.25),1) AS distance_q1_km,
            round(median(median_recorded_week_run_distance_meters/1000),1) AS distance_median_km,
            round(quantile_cont(median_recorded_week_run_distance_meters/1000,0.75),1) AS distance_q3_km,
            round(quantile_cont(median_recorded_week_run_count,0.25),2) AS frequency_q1,
            round(median(median_recorded_week_run_count),2) AS frequency_median,
            round(quantile_cont(median_recorded_week_run_count,0.75),2) AS frequency_q3,
            median(recorded_run_weeks) FILTER(WHERE recorded_run_weeks>0) AS median_contributing_run_weeks
            FROM selected GROUP BY ordinal,1,2 ORDER BY ordinal,2""")
        lines += ['## What remains unresolved', '',
            '- Differences can reflect posting/capture patterns, differing observed portions of the year, measurement availability, '
            'and selection into the source registry. They are not estimates of differences in complete training.',
            '- Source-warning counts are exposed, not used as a silent exclusion criterion. No warning does not prove complete capture.',
            '- Final comparison window, inclusion, uncertainty method, and performance tiers require review. '
            'Do not promote these exploratory tables to homepage findings.',
            '- Session pace, intensity, and steeple-specific technical work are not inferred from these two measures.', '',
            'Checks: one summary per directory athlete, all canonical Run records reconcile to summaries, '
            'and unavailable weekly distances remain null. The production artifact is opened read-only and unchanged.', '']
        if figure_data is not None:
            result = c.execute('''SELECT primary_discipline AS event, gender AS recorded_sex,
                recorded_run_weeks, distance_measured_weeks,
                median_recorded_week_run_distance_meters/1000 AS distance_km,
                median_recorded_week_run_count AS frequency
                FROM selected ORDER BY ordinal, gender, athlete_id''')
            names = [field[0] for field in result.description]
            payload = {'policy_version': POLICY_VERSION, 'source_database_sha256': source_hash,
                       'events': STUDY_EVENTS, 'athlete_summaries': [dict(zip(names,row)) for row in result.fetchall()]}
    if hashlib.sha256(database.read_bytes()).hexdigest() != source_hash:
        raise ValueError('Database changed during analysis')
    if figure_data is not None:
        figure_data.parent.mkdir(parents=True, exist_ok=True)
        figure_data.write_text(json.dumps(payload, indent=2, allow_nan=False) + '\n', encoding='utf-8')
    return '\n'.join(lines)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, default=ROOT / 'deploy/enduranceviz_2024.duckdb')
    parser.add_argument('--output', type=Path, default=ROOT / 'docs/p1-exploratory-training-2024.md')
    parser.add_argument('--figure-data', type=Path, help='Optional local figure inputs; use Git-ignored data/derived/2024/')
    args = parser.parse_args()
    args.output.write_text(analyze(args.database, args.figure_data), encoding='utf-8')
    print(f'Wrote {args.output}')
