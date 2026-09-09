#!/usr/bin/env python3
"""Publish aggregate collection/posting diagnostics; no eligibility thresholds."""

import argparse
import hashlib
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from enduranceviz.observability import ATHLETE_POSTING_SQL, WEEK_EVIDENCE_SQL


def table(connection, query):
    result = connection.execute(query)
    headers = [col[0].replace('_', ' ') for col in result.description]
    lines = ['| ' + ' | '.join(headers) + ' |', '| ' + ' | '.join('---' for _ in headers) + ' |']
    for row in result.fetchall():
        lines.append('| ' + ' | '.join('—' if x is None else str(x).replace('|', '\\|') for x in row) + ' |')
    return lines + ['']


def audit(database):
    with duckdb.connect(str(database), read_only=True) as c:
        # Temporary views live only in this connection; the database remains read-only.
        c.execute(f'CREATE TEMP VIEW evidence AS {WEEK_EVIDENCE_SQL}')
        c.execute(f'CREATE TEMP VIEW posting AS {ATHLETE_POSTING_SQL}')
        c.execute("CREATE TEMP VIEW selected AS SELECT * FROM posting WHERE primary_discipline IN ('800m', '1500m')")
        if c.execute('SELECT count(*) - count(DISTINCT athlete_id) FROM posting').fetchone()[0]:
            raise ValueError('Posting audit duplicates athletes')
        if c.execute('SELECT count(*) FROM posting WHERE full_calendar_weeks <> 52').fetchone()[0]:
            raise ValueError('Expected 52 full calendar weeks per athlete')
        posted = c.execute('SELECT sum(posted_activities) FROM evidence').fetchone()[0]
        source = c.execute('SELECT count(*) FROM activities_2024').fetchone()[0]
        if posted != source:
            raise ValueError('Evidence join lost or duplicated canonical activities')
        run_posts = c.execute('SELECT sum(posted_runs) FROM evidence').fetchone()[0]
        source_runs = c.execute("SELECT count(*) FROM activities_2024 WHERE activity_category = 'Run'").fetchone()[0]
        if run_posts != source_runs:
            raise ValueError('Evidence join lost or duplicated canonical runs')
        lines = [
            '# Collection and posting audit: 800m versus 1500m', '',
            f'Database SHA-256: `{hashlib.sha256(database.read_bytes()).hexdigest()}`', '',
            'Reproduce: `.venv/bin/python scripts/audit_observability_2024.py`', '',
            'Scope: all athletes whose P0 primary event is 800m or 1500m, including athletes excluded by the old '
            'coverage score. No annual-week cutoff or new inclusion rule is applied.', '',
            'Weekly diagnostics use the 52 full Monday–Sunday weeks in 2024. Annual activity counts also include '
            'December 30–31; that partial week is excluded only from the full-week diagnostics.', '',
            '## Definitions', '',
            '| Evidence state | What it establishes |', '| --- | --- |',
            '| activity_and_weekly_record | Activity records and a weekly record exist with no P0 source warning. This is not proof of complete capture. |',
            '| ambiguous_empty_record | Legacy weekly record exists but no activities; the old collector could emit this after an exception. Empty capture is unconfirmed. |',
            '| source_warning | P0 detected a disagreement or other source warning. Activity records, where present, are retained for inspection. |',
            '| activity_without_weekly_record | Actual activities exist without corresponding weekly collection evidence. |',
            '| no_collection_evidence | Neither activities nor an observed weekly record exist. |', '',
            '“Recorded runs” and “weeks with runs” count records in the snapshot. Zero records does not establish '
            'zero training or deliberate non-posting. Longest gaps include gaps at both ends of the year. '
            'Known distance sums omit unavailable measurements and are not asserted to be complete totals.', '',
            '## Athlete inventory', '',
        ]
        lines += table(c, '''SELECT primary_discipline AS event, gender AS recorded_sex, count(*) AS registry,
            count(*) FILTER (WHERE legacy_p0_eligible) AS legacy_eligible,
            count(*) FILTER (WHERE recorded_runs_2024 > 0) AS any_recorded_runs,
            count(*) FILTER (WHERE recorded_activities_2024 = 0) AS no_recorded_activities
            FROM selected GROUP BY 1,2 ORDER BY 1,2''')
        lines += ['## Full athlete-week evidence', '']
        lines += table(c, '''SELECT evidence_state, count(*) AS athlete_weeks,
            count(*) FILTER (WHERE e.posted_activities > 0) AS with_activities,
            count(*) FILTER (WHERE s.legacy_p0_eligible) AS legacy_eligible_athlete_weeks
            FROM evidence e JOIN selected s USING (athlete_id) WHERE NOT e.is_partial_window GROUP BY 1 ORDER BY 1''')
        lines += ['## Posting counts — diagnostic bins, not eligibility categories', '',
                  'These bins describe the shape of the stored data. They do not classify athletes as race-only, '
                  'complete posters, or suitable for an analysis. No training-volume comparison is calculated.', '']
        lines += table(c, '''SELECT primary_discipline AS event,
            CASE WHEN recorded_runs_2024 = 0 THEN '0' WHEN recorded_runs_2024 <= 10 THEN '1–10'
                 WHEN recorded_runs_2024 <= 100 THEN '11–100' ELSE '101+' END AS annual_recorded_runs,
            count(*) AS athletes, count(*) FILTER (WHERE legacy_p0_eligible) AS legacy_eligible
            FROM selected GROUP BY 1,2 ORDER BY 1,2''')
        lines += ['## Source warnings', '']
        lines += table(c, '''SELECT e.collection_error_code, count(*) AS athlete_weeks,
            count(*) FILTER (WHERE e.posted_activities > 0) AS with_activities
            FROM evidence e JOIN selected s USING (athlete_id)
            WHERE NOT e.is_partial_window AND e.evidence_state = 'source_warning' GROUP BY 1 ORDER BY 2 DESC''')
        lines += ['## Two traced examples', '',
                  'These are the examples already discussed with Daniel, not representative case studies. '
                  'Names identify source records; no posting intention is inferred.', '']
        lines += table(c, '''SELECT s.display_name, e.week_start_utc, e.legacy_coverage_status,
            e.evidence_state, e.posted_activities, e.posted_runs, e.evidence_source
            FROM evidence e JOIN selected s USING (athlete_id)
            WHERE (s.display_name = 'Jack Balick' AND e.week_start_utc = DATE '2024-01-01')
               OR (s.display_name = 'Abdurrahman Gediklioğlu' AND e.week_start_utc = DATE '2024-01-15')
            ORDER BY s.display_name''')
        lines += ['## Interpretation and next work', '',
                  '- The 26/39-week proposal is withdrawn. Annual coverage is not a prerequisite for exploration.',
                  '- A legacy high/moderate score is not an approved P1 inclusion filter.',
                  '- Reconcile source conflicts and inspect activity calendars before choosing analysis windows.',
                  '- Activity-only weeks remain evidence of posted activity, even when weekly summaries are absent.',
                  '- A successful collection record cannot establish completeness of actual athlete training.',
                  '- This diagnostic layer does not rebuild the P0 artifact or change deployed profile metrics. '
                  'The P0 ambiguous-zero behaviour remains a known limitation until a separately validated pipeline correction.', '']
    return '\n'.join(lines)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, default=ROOT / 'deploy/enduranceviz_2024.duckdb')
    parser.add_argument('--output', type=Path, default=ROOT / 'docs/p1-observability-audit-2024.md')
    args = parser.parse_args()
    args.output.write_text(audit(args.database), encoding='utf-8')
    print(f'Wrote {args.output}')
