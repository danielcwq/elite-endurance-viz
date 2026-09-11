#!/usr/bin/env python3
"""Reproduce a local full recording inventory and explicit anomaly-review candidates."""
import argparse
import hashlib
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))
from enduranceviz.serving import ServingRepository, PACKAGED_DATABASE

CANDIDATES={
    'Erik Hille':'High recorded volume with broad recording: review training blocks, not just a year-level average.',
    'Romain Legendre':'High recorded volume within the assigned 5000m group; compare with marathon-assigned accounts.',
    'Caden Norris':'Low recorded distance despite many Run records: inspect short records and recording granularity before interpreting training.',
    'Sarah McDonald':'Low recorded distance across many weeks, but median one Run record/week: breadth is not dense capture.',
    'Rebecca Bassett':'Low recorded distance in the 5000m group despite 49 recorded weeks; posting completeness remains unknown.',
    'Laura Nagel':'Dense year-long recording, yet many source-warning weeks: collection-summary reliability differs from activity evidence.',
    'Samuel Barata':'Assigned to 10000m despite stored half-marathon and marathon results: event assignment is not a unique specialty.',
    'Kate Mitchell':'IDENTITY REVIEW FIRST: linked account mentions a 5 km time of 32:12 and a goal below 30 minutes; registry has 800m 2:01.13 (1142 points). Possible same-name match, not a low-mileage elite claim.',
}

def audit(database):
    path=Path(database);before=hashlib.sha256(path.read_bytes()).hexdigest()
    repo=ServingRepository(path);all_rows=repo.recording_summaries()
    recorded=[r for r in all_rows if r['recorded_activities_2024']>0]
    def cell(value): return str(value).replace('|','\\|').replace('\n',' ')
    def table(rows):
        lines=['| Athlete / profile | Assigned event | Sex | Run weeks | Distance weeks | Median km/wk | Median Run records/wk | Annual Run records | Warning Run weeks |',
               '|---|---|---|---:|---:|---:|---:|---:|---:|']
        for r in rows:
            distance=r['median_recorded_week_run_distance_meters']
            name=f"[{cell(r['name'])}](http://127.0.0.1:8011/athlete/{r['athlete_id']})"
            lines.append('| '+' | '.join(map(cell,[name,r['primary_discipline'] or 'Unassigned',r['gender'],
                r['recorded_run_weeks'],r['distance_measured_weeks'],f'{distance/1000:.2f}' if distance is not None else 'Unavailable',
                r['median_recorded_week_run_count'] if r['median_recorded_week_run_count'] is not None else 'Unavailable',
                r['recorded_runs_2024'],r['source_warning_run_weeks']]))+' |')
        return lines
    lines=['# Recorded athlete inventory and candidate questions', '',f'Database SHA-256: `{before}`.', '',
        f'{len(all_rows):,} registry athletes; {len(recorded)} have stored activities; '
        f"{sum(r['recorded_runs_2024']>0 for r in recorded)} have stored Runs.", '',
        'This inventories the snapshot, not current Strava visibility, every public athlete, '
        'or every structured workout. There is no validated workout/session label. '
        'The existing account links are not independently reverified by this audit.', '',
        '## Proposed examples—not selected representative case studies', '',
        'These are purposive inspection candidates spanning volume, posting density, source '
        'warnings, event ambiguity, and possible identity mismatch. They are not a population '
        'ranking. The initial high/low-volume screen considered at least 40 distance-measured '
        'weeks only to find broadly recorded examples; this is not an eligibility cutoff and '
        'does not filter the full inventory or comparison UI.', '']
    for name,reason in CANDIDATES.items():
        matches=[r for r in recorded if r['name']==name]
        if not matches: continue
        lines += [f'### {name}', '',reason,'',*table(matches),'',
                  'Stored results: '+ '; '.join(r['event_results'] or 'Unavailable' for r in matches)+'.','']
    year=[r for r in recorded if r['recorded_run_weeks']==52]
    lines += ['## Every account with Runs in all 52 full weeks','',
              'Sorted by distance-measured weeks, then name. This demonstrates continuity only, '
              'not complete training. Source warnings and record density must be inspected separately.','',
              *table(sorted(year,key=lambda r:(-r['distance_measured_weeks'],r['name']))),'',
              '## Full inventory—every athlete with at least one stored activity','',
              *table(sorted(recorded,key=lambda r:r['name'].casefold())),'']
    if hashlib.sha256(path.read_bytes()).hexdigest()!=before: raise RuntimeError('Database changed')
    return '\n'.join(lines)

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database',type=Path,default=PACKAGED_DATABASE)
    parser.add_argument('--output',type=Path,default=ROOT/'data/derived/2024/p1-exploration/recorded-athlete-audit.md')
    args=parser.parse_args();args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(audit(args.database),encoding='utf-8')
    print(f'Wrote local audit: {args.output}')
