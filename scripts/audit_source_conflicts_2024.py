#!/usr/bin/env python3
"""Trace weekly source conflicts and calendar posting counts without cohort cutoffs."""

import argparse
import hashlib
import sys
from collections import Counter
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from enduranceviz.analytics import load_weekly_evidence, select_weekly_evidence
from enduranceviz.source_audit import FIELDS, classify_conflict, text_value, numeric_value, time_value
from enduranceviz.observability import ATHLETE_POSTING_SQL
from scripts.audit_observability_2024 import table


def audit(database):
    database_hash = hashlib.sha256(database.read_bytes()).hexdigest()
    with duckdb.connect(str(database), read_only=True) as c:
        accounts = c.execute("""SELECT e.external_account_id, d.athlete_id::VARCHAR AS athlete_id,
            coalesce(d.display_name, d.official_name) AS name
            FROM athlete_external_accounts e JOIN athlete_directory_2024 d USING (athlete_id)
            WHERE d.primary_discipline IN ('800m', '1500m')""").fetchdf()
        if accounts.external_account_id.duplicated().any():
            raise ValueError('Ambiguous account mapping')
        account_map = accounts.set_index('external_account_id').to_dict('index')
        paths = list((ROOT / 'data/raw_data').glob('*.csv')) + list((ROOT / 'data/tempdata').glob('metadata_*.csv'))
        evidence = load_weekly_evidence(paths, ROOT)
        evidence = evidence[evidence.external_account_id.isin(account_map)]
        selected = select_weekly_evidence(evidence)
        selected = selected.set_index(['external_account_id', 'week_start_utc'])
        expected = c.execute("""SELECT c.athlete_id::VARCHAR, c.week_start_utc FROM data_coverage_2024 c
            JOIN athlete_directory_2024 d USING (athlete_id)
            WHERE d.primary_discipline IN ('800m','1500m') AND NOT c.is_partial_window
            AND contains(c.collection_error_code, 'CONFLICTING_WEEKLY_SOURCE_ROWS')""").fetchall()
        actual, counts, examples = set(), Counter(), {}
        selected_no_data = 0
        rounding_compatible = 0
        for key, group in evidence.groupby(['external_account_id', 'week_start_utc'], sort=True):
            if key[1].year != 2024 or key[1].month == 12 and key[1].day == 30:
                continue
            records = group.to_dict('records')
            kind = classify_conflict(records)
            if kind == 'identical':
                continue
            athlete = account_map[key[0]]
            actual.add((athlete['athlete_id'], key[1]))
            counts[kind] += 1
            if kind == 'different_totals_or_missing_fields':
                numeric = [(numeric_value(row.get('Distance (km)')), numeric_value(row.get('Elevation (m)')))
                           for row in records]
                if all(distance[0] == elevation[0] == 'number' for distance, elevation in numeric):
                    rounded = {(distance[1].quantize(Decimal('0.1'), rounding=ROUND_HALF_UP),
                                elevation[1].quantize(Decimal('1'), rounding=ROUND_HALF_UP),
                                time_value(row.get('Time')))
                               for (distance, elevation), row in zip(numeric, records)}
                    rounding_compatible += int(len(rounded) == 1)
            winner = selected.loc[key]
            selected_no_data += int(bool(winner.reports_no_data))
            if kind not in examples:
                examples[kind] = (athlete['name'], key[1], winner.evidence_source, records)
        if actual != set(expected):
            raise ValueError('Source conflict keys do not reconcile with packaged warnings')
        c.execute(f'CREATE TEMP VIEW posting AS {ATHLETE_POSTING_SQL}')
        c.execute("CREATE TEMP VIEW selected AS SELECT * FROM posting WHERE primary_discipline IN ('800m','1500m')")
        lines = ['# Weekly source conflicts and posting calendars', '',
            f'Database SHA-256: `{database_hash}`', '',
            'Reproduce: `.venv/bin/python scripts/audit_source_conflicts_2024.py`', '',
            'Scope: all athletes assigned to 800m/1500m. No P0 coverage filter or annual-week cutoff. '
            'Sources are unchanged; classifications do not clear warnings or choose a different source.', '',
            '## Source disagreements', '',
            f'Original source rows reproduce the exact {len(actual):,} full athlete-week conflict keys in the packaged database. '
            f'The classification table counts {sum(counts.values()):,} account-weeks; an athlete can own multiple accounts.', '',
            '| Diagnostic category | Account-weeks |', '| --- | ---: |']
        lines += [f'| {kind} | {count} |' for kind, count in sorted(counts.items())]
        lines += ['', f'In {selected_no_data:,} conflicting account-weeks, the existing selection rule chooses a `No Data` row.', '',
            'Categories are exclusive, in order: No Data versus another summary; differing date-range text; '
            'different totals or missing fields; otherwise equivalent numeric/time formatting. Numeric comparison uses '
            'exact decimal equality, not an arbitrary tolerance. A differing total does not by itself identify the correct source.', '',
            f'As a secondary diagnostic, {rounding_compatible} of the differing-total account-weeks agree after '
            'rounding distance to 0.1 km and elevation to 1 m, with equal parsed time. These precisions match the '
            'display-style values in the examples; they are not approved error tolerances and do not clear warnings. '
            'The old collector converts displayed miles by 1.60934 and feet by 0.3048 '
            '(`Get_Data/strava_scrape_new.py:352–383`), so unit conversion can produce extra decimal places. '
            'Compatibility with rounding is not proof that two captures are identical.', '',
            '## Traceable examples', '',
            'One example per category, selected deterministically by account/week order. These illustrate source defects, '
            'not representative athlete case studies. The existing source preference is not proof of correctness.', '']
        for kind, (name, week, winner, records) in sorted(examples.items()):
            lines += [f'### {kind}: {name}, week starting {week}', '', f'Existing selected source: `{winner}`.', '',
                '| File and CSV row | Date range | Distance (km) | Time | Elevation (m) |', '| --- | --- | ---: | --- | ---: |']
            # Distinct source values only; preserve their first exact file/row locator.
            seen = set()
            for row in records:
                signature = tuple(text_value(row.get(field)) for field in FIELDS)
                if signature in seen:
                    continue
                seen.add(signature)
                values = [f"{row['source_file']}:{row['source_row_number']}", *signature]
                lines.append('| ' + ' | '.join(value.replace('|', '\\|') for value in values) + ' |')
            lines.append('')
        lines += ['## Posting calendars', '',
            'The next table counts athletes with at least one stored Run in each UTC calendar month. '
            'Months are display bins, not inclusion requirements. This uses activity timestamps, not weekly summaries, '
            'and includes December 30–31. It cannot establish intentional race-only or sporadic posting.', '']
        lines += table(c, """WITH months AS (SELECT unnest(generate_series(DATE '2024-01-01', DATE '2024-12-01', INTERVAL '1 month')) AS month),
            events AS (SELECT DISTINCT primary_discipline AS event FROM selected),
            counts AS (SELECT date_trunc('month', a.start_at_utc AT TIME ZONE 'UTC') AS month,
                s.primary_discipline AS event, count(DISTINCT a.athlete_id) AS athletes
                FROM activities_2024 a JOIN selected s ON a.athlete_id::VARCHAR = s.athlete_id
                WHERE a.activity_category='Run' GROUP BY 1,2)
            SELECT strftime(m.month, '%Y-%m') AS month, e.event, coalesce(c.athletes,0) AS athletes_with_recorded_runs
            FROM months m CROSS JOIN events e LEFT JOIN counts c USING(month,event) ORDER BY 1,2""")
        lines += ['## Within-year gaps among accounts with recorded runs', '',
            'Full-week diagnostics below exclude only the two-day final week. Counts are distributions among athletes '
            'with any recorded 2024 run, not proposed eligibility cutoffs. Gaps include the beginning and end of the year.', '']
        lines += table(c, """SELECT primary_discipline AS event, count(*) AS athletes,
            min(weeks_with_runs) AS minimum_run_weeks, median(weeks_with_runs) AS median_run_weeks,
            max(weeks_with_runs) AS maximum_run_weeks,
            median(longest_no_recorded_run_gap) AS median_longest_gap_weeks
            FROM selected WHERE recorded_runs_2024 > 0 GROUP BY 1 ORDER BY 1""")
        lines += ['', '## Consequences for P1', '',
            '- Do not discard actual activity records because a weekly CSV contradicts them.',
            '- Do not clear source warnings merely because a later or lexically preferred file exists.',
            '- Posting calendars describe stored observations. They cannot label an athlete race-only without additional evidence.',
            '- Choose the comparison question before deciding whether any minimum observation window is relevant.', '']
    if hashlib.sha256(database.read_bytes()).hexdigest() != database_hash:
        raise ValueError('Source database changed during audit')
    return '\n'.join(lines)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, default=ROOT / 'deploy/enduranceviz_2024.duckdb')
    parser.add_argument('--output', type=Path, default=ROOT / 'docs/p1-source-conflicts-2024.md')
    args = parser.parse_args()
    args.output.write_text(audit(args.database), encoding='utf-8')
    print(f'Wrote {args.output}')
