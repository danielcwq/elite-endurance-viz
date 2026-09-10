#!/usr/bin/env python3
"""Local serial before/after repository and in-process ASGI timings; never a remote load test."""

import argparse
import hashlib
import json
import logging
import os
import platform
import re
import statistics
import subprocess
import sys
import time
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import duckdb
from starlette.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from enduranceviz.activity_filters import ActivityFilters
from enduranceviz.serving import ServingRepository

BASELINE = '22cce81cb19493caed489acdaa78608d5363196e'


def load_baseline(ref):
    if not re.fullmatch(r'[0-9a-f]{40}',ref):
        raise ValueError('Baseline must be a full local Git commit SHA')
    source = subprocess.run(['git','show',f'{ref}:enduranceviz/serving.py'],cwd=ROOT,
                            text=True,capture_output=True,check=True).stdout
    module = types.ModuleType('benchmark_committed_serving')
    module.__file__ = str(ROOT/'enduranceviz/serving.py')
    # Execute only the explicitly selected local repository revision, not web content.
    exec(compile(source,f'{ref}:enduranceviz/serving.py','exec'),module.__dict__)
    return module.ServingRepository, hashlib.sha256(source.encode()).hexdigest()


def select_cases(database):
    with duckdb.connect(str(database),read_only=True) as c:
        active = c.execute('''SELECT athlete_id::VARCHAR,count(*) AS n FROM activities_2024
            GROUP BY 1 ORDER BY n,1''').fetchall()
        empty = c.execute('''SELECT d.athlete_id::VARCHAR FROM athlete_directory_2024 d
            WHERE NOT EXISTS(SELECT 1 FROM activities_2024 a WHERE a.athlete_id=d.athlete_id)
            ORDER BY d.athlete_id LIMIT 1''').fetchone()
    if not active or not empty:
        raise ValueError('Benchmark requires both active and no-record athletes')
    typical, largest = active[len(active)//2], active[-1]
    return [
        ('median_positive_history',*typical,1,ActivityFilters()),
        ('largest_history_first_page',*largest,1,ActivityFilters()),
        ('largest_history_last_page',*largest,10**6,ActivityFilters()),
        ('largest_history_one_day_swim_filter',*largest,1,ActivityFilters.parse('2024-01-01','2024-01-01','Swim')),
        ('no_record_history',empty[0],0,1,ActivityFilters()),
    ]


def paired_measure(before,after,iterations,warmups):
    durations = {'before':[],'after':[]}
    outputs = {}
    for i in range(warmups+iterations):
        order = (('before',before),('after',after))
        if i%2:
            order = tuple(reversed(order))
        for label,fn in order:
            start = time.perf_counter_ns()
            value = fn()
            elapsed = (time.perf_counter_ns()-start)/1_000_000
            outputs[label] = value
            if i>=warmups:
                durations[label].append(elapsed)
        if outputs['before'] != outputs['after']:
            raise ValueError('Before/after results differ; benchmark is not a valid equivalent-operation comparison')
    return durations,outputs['after']


def summarize(samples):
    return dict(median_ms=round(statistics.median(samples),3),
                p95_ms=round(sorted(samples)[max(0, int(.95*len(samples)+.999999)-1)],3),
                min_ms=round(min(samples),3),max_ms=round(max(samples),3))


def benchmark(database,iterations=30,warmups=3,baseline=BASELINE):
    if not 5<=iterations<=100 or not 1<=warmups<=10:
        raise ValueError('Use 5–100 measured pairs and 1–10 warm-up pairs')
    database = Path(database).resolve()
    source_hash = hashlib.sha256(database.read_bytes()).hexdigest()
    old_class, old_hash = load_baseline(baseline)
    old, new = old_class(database),ServingRepository(database)
    # Import the app against this explicit DB, even if the caller configured another one.
    with patch.dict(os.environ,{'ENDURANCEVIZ_DB_PATH':str(database)}):
        import main
    results = []
    with TestClient(main.app) as client, patch.object(logging.getLogger('enduranceviz.requests'),'disabled',True):
        for label,athlete_id,history_count,page,filters in select_cases(database):
            durations,value = paired_measure(
                lambda: old.activities(athlete_id,page=page,filters=filters),
                lambda: new.activities(athlete_id,page=page,filters=filters),iterations,warmups)
            query = {name:summarize(values) for name,values in durations.items()}
            def request(repository):
                with patch.object(main,'repository',repository):
                    response = client.get(filters.url(athlete_id,page).split('#')[0])
                if response.status_code!=200:
                    raise ValueError(f'Unexpected profile response: {response.status_code}')
                return response.content
            durations,body = paired_measure(lambda:request(old),lambda:request(new),iterations,warmups)
            results.append(dict(case=label,history_records=history_count,matched_records=value['total'],
                                returned_rows=len(value['rows']),returned_page=value['page'],
                                query=query,profile={name:summarize(values) for name,values in durations.items()},
                                before_html_bytes=len(body),after_html_bytes=len(body),
                                equivalent_results_and_html=True))
    if hashlib.sha256(database.read_bytes()).hexdigest()!=source_hash:
        raise ValueError('Database changed during benchmark')
    revision = subprocess.run(['git','rev-parse','HEAD'],cwd=ROOT,capture_output=True,text=True,check=True).stdout.strip()
    return dict(timestamp_utc=datetime.now(timezone.utc).isoformat(),database_sha256=source_hash,
                baseline_commit=baseline,baseline_serving_sha256=old_hash,current_head=revision,
                current_serving_sha256=hashlib.sha256((ROOT/'enduranceviz/serving.py').read_bytes()).hexdigest(),
                python=platform.python_version(),duckdb=duckdb.__version__,platform=platform.platform(),
                cpu_count=os.cpu_count(),iterations=iterations,warmup_pairs=warmups,results=results)


def report(payload):
    lines = ['# Local activity-serving before/after measurements','',
        'This measures the P1 activity-pagination connection change, not the 2025 Mongo/FastHTML site '
        'or a production deployment. It uses the same read-only database and current page renderer on both sides.','',
        f"Measured UTC: `{payload['timestamp_utc']}`. Baseline repository: `{payload['baseline_commit']}`.",
        f"Current HEAD at measurement: `{payload['current_head']}`; the changed serving file is identified by its hash below.",'',
        f"Database SHA-256: `{payload['database_sha256']}`.",
        f"Baseline serving module SHA-256: `{payload['baseline_serving_sha256']}`.",
        f"Measured current serving module SHA-256: `{payload['current_serving_sha256']}`.",'',
        f"Environment: {payload['platform']}; Python {payload['python']}; DuckDB {payload['duckdb']}; "
        f"{payload['cpu_count']} logical CPUs reported. Other local processes were not stopped.",'',
        '## Method','',
        f"{payload['iterations']} serial measured before/after pairs per operation, after {payload['warmup_pairs']} "
        'warm-up pairs. Execution order alternates on every pair. No parallel requests, cache flushes, '
        'cold-start simulation, or network requests are made. All timings use a monotonic high-resolution clock.','',
        'Repository timings include connection setup, two SQL queries, row materialization, and pagination bookkeeping. '
        'Before: a new read-only connection per query. After: one request-local read-only connection for the count and rows. '
        'No global connection pool or result cache is introduced. Connections close before returning.','',
        'Profile timings use in-process ASGI requests, including current HTML rendering and middleware. '
        'Metadata/timeline caches are warmed, while activity pagination remains uncached. Request logging output '
        'is disabled only in this benchmark process. These are not browser-load, TCP, TLS, CDN, cold-start, or Vercel latency measurements.','',
        'Cases are selected deterministically by activity count and UUID: the upper-middle positive history, '
        'largest history (first and out-of-range/clamped-last page), a one-day Swim filter on the largest history, '
        'and a registry athlete with no activity records. No athlete IDs or response bodies are exported. '
        'Every pair must return identical repository objects and byte-identical HTML, otherwise the run fails.','',
        '## Measured operations','',
        '| Case | History / matched / returned records | Repository median before → after (ms) | Repository p95 before → after (ms) | Profile median before → after (ms) | Profile p95 before → after (ms) | HTML bytes before → after |',
        '| --- | --- | --- | --- | --- | --- | --- |']
    for r in payload['results']:
        cells = [r['case'],f"{r['history_records']} / {r['matched_records']} / {r['returned_rows']}"]
        for kind,metric in (('query','median_ms'),('query','p95_ms'),('profile','median_ms'),('profile','p95_ms')):
            cells.append(f"{r[kind]['before'][metric]} → {r[kind]['after'][metric]}")
        cells.append(f"{r['before_html_bytes']} → {r['after_html_bytes']}")
        lines.append('| '+' | '.join(cells)+' |')
    lines += ['', 'p95 is the nearest-rank sample percentile, not an uncertainty interval. '
              'Small local timing samples can vary across machines and runs; no CI timing threshold is imposed.', '',
              '## Reproduce','', '```sh',
              '.venv/bin/python scripts/benchmark_activity_serving.py --iterations 30 --warmups 3', '```','',
              'Requires the full baseline commit above to exist in local Git history. The script executes that '
              'committed serving module as the baseline; use only trusted repository revisions. '
              'It does not fetch, check out, reset, or modify the worktree. Optional `--json` writes aggregate '
              'samples summaries and fingerprints; no athlete-level export is created.','',
              'Production and the packaged database were not modified. Payload size is unchanged by design. '
              'Hosted staging load, cold starts, and browser performance remain separate P1 work.', '']
    return '\n'.join(lines)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database',type=Path,default=ROOT/'deploy/enduranceviz_2024.duckdb')
    parser.add_argument('--baseline',default=BASELINE)
    parser.add_argument('--iterations',type=int,default=30)
    parser.add_argument('--warmups',type=int,default=3)
    parser.add_argument('--output',type=Path,default=ROOT/'docs/p1-serving-performance.md')
    parser.add_argument('--json',type=Path)
    args = parser.parse_args()
    result = benchmark(args.database,args.iterations,args.warmups,args.baseline)
    args.output.write_text(report(result),encoding='utf-8')
    if args.json:
        args.json.parent.mkdir(parents=True,exist_ok=True)
        args.json.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(f'Wrote {args.output}')
