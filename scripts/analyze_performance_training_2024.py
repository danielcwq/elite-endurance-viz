#!/usr/bin/env python3
"""Audit continuous-point/recorded-training pairs; optionally export local plot inputs."""

import argparse
import hashlib
import json
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from enduranceviz.performance_training import (
    PERFORMANCE_POLICY_VERSION, PERFORMANCE_TRAINING_SQL, PAIRED_METRICS_SQL,
)
from enduranceviz.recorded_training import ATHLETE_RECORDED_TRAINING_SQL, POLICY_VERSION, STUDY_EVENTS
from scripts.audit_observability_2024 import table


def prepare(connection):
    c = connection
    c.execute(f'CREATE TEMP VIEW recorded_athletes AS {ATHLETE_RECORDED_TRAINING_SQL}')
    c.execute(f'CREATE TEMP VIEW performance_training AS {PERFORMANCE_TRAINING_SQL}')
    c.execute('CREATE TEMP TABLE event_order(event VARCHAR, ordinal INTEGER)')
    c.executemany('INSERT INTO event_order VALUES (?, ?)', [(e, i) for i, e in enumerate(STUDY_EVENTS)])
    c.execute('''CREATE TEMP VIEW selected AS SELECT a.*, e.ordinal
        FROM performance_training a JOIN event_order e ON a.primary_discipline=e.event''')
    c.execute(f'CREATE TEMP VIEW metric_inventory AS {PAIRED_METRICS_SQL}')
    count, unique = c.execute('SELECT count(*),count(DISTINCT athlete_id) FROM performance_training').fetchone()
    if count != unique or count != c.execute('SELECT count(*) FROM athlete_directory_2024').fetchone()[0]:
        raise ValueError('Performance join lost or duplicated directory athletes')
    if c.execute('''SELECT count(*) FROM performances_2024 p JOIN athlete_directory_2024 a USING(athlete_id)
        WHERE p.gender<>a.gender''').fetchone()[0]:
        raise ValueError('Performance and registry recorded-sex classifications disagree')
    if c.execute('SELECT count(*) FROM performances_2024 WHERE results_score<=0').fetchone()[0]:
        raise ValueError('Nonpositive performance score')
    if c.execute('''SELECT count(*) FROM metric_inventory WHERE metric_value IS NOT NULL
        AND (NOT isfinite(metric_value) OR metric_value<0 OR contributing_weeks NOT BETWEEN 1 AND 52)''').fetchone()[0]:
        raise ValueError('Invalid metric value or contributing-week denominator')


def analyze(database, figure_data=None):
    database = Path(database)
    source_hash = hashlib.sha256(database.read_bytes()).hexdigest()
    with duckdb.connect(str(database), read_only=True) as c:
        prepare(c)
        lines = [
            '# Continuous performance points and recorded training: exploratory view', '',
            f'Score policy: `{PERFORMANCE_POLICY_VERSION}`. Training policy: `{POLICY_VERSION}`.', '',
            f'Source database SHA-256: `{source_hash}`.', '',
            '## Question and definitions', '',
            'Within each primary event and recorded-sex group, what does the relationship between stored '
            '2024 World Athletics result points and recorded-week running summaries look like?', '',
            'Daniel approved continuous points, separate sex groups, and the existing recorded-week summaries. '
            'The operational score definition below was disclosed before calculating pairs. '
            'This is an exploratory scatter view, not a fitted association model or a final study conclusion.', '',
            '- One athlete contributes at most one point to each metric panel in their P0 primary event. '
            'Use the highest non-null `results_score` among stored 2024 performances in that event. '
            'Ties have the same score and do not create additional entries. Other-event results never replace a missing score.',
            '- Keep athletes without a score or training metric in the inventory; omit only unavailable pairs from the relevant plot. '
            'There is no new point, annual-week, or P0 eligibility cutoff.',
            '- Training values are each athlete’s median across full UTC weeks with recorded Runs. '
            'Distance uses only weeks where all Run distances are finite and nonnegative; Run-record counts use all recorded-running weeks. '
            'December 30–31 is excluded from these medians. Records are not independent sessions.',
            '- Colour shows the number of weeks contributing to that particular metric, on a fixed 1–52 scale. '
            'It is not a confidence score. Actual activity records from source-warning weeks remain.', '',
            'The snapshot’s pre-existing 1,100-point source floor restricts the available performance range. '
            '“Highest stored score” is not a claim to have every race in an athlete’s season. '
            'Primary-event assignment itself uses performance points. These limitations and selective public posting '
            'constrain interpretation; do not pool event/sex panels or treat training summaries as complete years. '
            'No p-values, fitted lines, correlation coefficients, or causal/predictive claims are calculated.', '',
            '## Reproduce locally', '',
            '```sh',
            '.venv/bin/python scripts/analyze_performance_training_2024.py --figure-data data/derived/2024/p1-exploration/performance-training.json',
            '.venv/bin/python scripts/plot_performance_training_2024.py data/derived/2024/p1-exploration/performance-training.json data/derived/2024/p1-exploration/performance-training',
            '```', '',
            'The plot command writes one distance figure and one Run-record-count figure. Install '
            '`requirements-analysis.txt` for plotting. Individual plot inputs and images stay local under Git-ignored '
            '`data/derived/`; this report contains aggregate counts only. Nothing is deployed.', '',
            '## Score availability', '',
        ]
        lines += table(c, '''SELECT primary_discipline AS event, gender AS recorded_sex,
            count(*) AS registry, count(best_stored_results_score) AS scored_athletes,
            count(*) FILTER(WHERE best_stored_results_score IS NULL) AS missing_score,
            sum(stored_performance_count) AS primary_event_results,
            sum(stored_performance_count-scored_performance_count) AS unscored_results,
            min(best_stored_results_score) AS score_min, max(best_stored_results_score) AS score_max
            FROM selected GROUP BY ordinal,1,2 ORDER BY ordinal,2''')
        lines += ['## Pair availability by metric', '',
            'The four pair states partition each event/sex registry. Missing-score-only means training is available; '
            'missing-training-only means a score is available. No training value is filled with zero.', '']
        lines += table(c, '''SELECT primary_discipline AS event, gender AS recorded_sex, metric,
            count(*) AS registry,
            count(*) FILTER(WHERE best_stored_results_score IS NOT NULL AND metric_value IS NOT NULL) AS paired,
            count(*) FILTER(WHERE best_stored_results_score IS NULL AND metric_value IS NOT NULL) AS missing_score_only,
            count(*) FILTER(WHERE best_stored_results_score IS NOT NULL AND metric_value IS NULL) AS missing_training_only,
            count(*) FILTER(WHERE best_stored_results_score IS NULL AND metric_value IS NULL) AS missing_both
            FROM metric_inventory GROUP BY ordinal,1,2,3 ORDER BY ordinal,2,3''')
        lines += ['## Posting breadth among paired athletes', '',
            'Week counts are metric-specific. Warning counts indicate retained source-warning running weeks, '
            'not that every plotted metric week has a warning. Inspect sparse and well-documented points together '
            'before deciding whether any numerical association summary is useful.', '']
        lines += table(c, '''SELECT primary_discipline AS event, gender AS recorded_sex, metric,
            count(*) AS paired, min(best_stored_results_score) AS paired_score_min,
            max(best_stored_results_score) AS paired_score_max,
            min(contributing_weeks) AS weeks_min, median(contributing_weeks) AS weeks_median,
            max(contributing_weeks) AS weeks_max,
            count(*) FILTER(WHERE source_warning_run_weeks>0) AS athletes_with_warning_run_weeks
            FROM metric_inventory WHERE best_stored_results_score IS NOT NULL AND metric_value IS NOT NULL
            GROUP BY ordinal,1,2,3 ORDER BY ordinal,2,3''')
        omitted = c.execute("SELECT count(*) FROM selected WHERE gender NOT IN ('female','male')").fetchone()[0]
        lines += [f'Other/unknown recorded-sex registry athletes: {omitted}. They remain in the tables but are not pooled '
                  'into the approved women’s/men’s figures.', '',
                  'Next analytical checkpoint: inspect the scatter view before choosing any numerical association '
                  'summary, coverage sensitivity analysis, or final inclusion rule. '
                  'See [analysis decisions](p1-analysis-decisions.md).', '']
        result = c.execute('''SELECT primary_discipline AS event, gender AS recorded_sex,
            best_stored_results_score AS points, recorded_run_weeks, distance_measured_weeks,
            source_warning_run_weeks,
            median_recorded_week_run_distance_meters/1000 AS distance_km,
            median_recorded_week_run_count AS run_records
            FROM selected ORDER BY ordinal,gender,athlete_id''')
        names = [column[0] for column in result.description]
        payload = dict(performance_policy_version=PERFORMANCE_POLICY_VERSION, training_policy_version=POLICY_VERSION,
                       source_database_sha256=source_hash, events=STUDY_EVENTS,
                       athlete_summaries=[dict(zip(names, row)) for row in result.fetchall()])
    if hashlib.sha256(database.read_bytes()).hexdigest() != source_hash:
        raise ValueError('Database changed during analysis')
    if figure_data is not None:
        figure_data = Path(figure_data)
        figure_data.parent.mkdir(parents=True, exist_ok=True)
        figure_data.write_text(json.dumps(payload, indent=2, allow_nan=False) + '\n', encoding='utf-8')
    return '\n'.join(lines)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, default=ROOT / 'deploy/enduranceviz_2024.duckdb')
    parser.add_argument('--output', type=Path, default=ROOT / 'docs/p1-performance-training-2024.md')
    parser.add_argument('--figure-data', type=Path, help='Use Git-ignored data/derived/2024/ for local plot inputs')
    args = parser.parse_args()
    args.output.write_text(analyze(args.database, args.figure_data), encoding='utf-8')
    print(f'Wrote {args.output}')
