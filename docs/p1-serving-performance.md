# Local activity-serving before/after measurements

This measures the P1 activity-pagination connection change, not the 2025 Mongo/FastHTML site or a production deployment. It uses the same read-only database and current page renderer on both sides.

Measured UTC: `2026-09-10T00:10:27.536936+00:00`. Baseline repository: `22cce81cb19493caed489acdaa78608d5363196e`.
Current HEAD at measurement: `22cce81cb19493caed489acdaa78608d5363196e`; the changed serving file is identified by its hash below.

Database SHA-256: `047931ce47ea65940011137109b81b73248abd8f4bff33ca54d3c355ed8a970c`.
Baseline serving module SHA-256: `f39f66727d9b5b5ad51e8986c39784894298b95ced50ff64a4b4d44f27f55230`.
Measured current serving module SHA-256: `a0d841dd5a690722ebb1855a9ebcd7399a420727f62982f07c50f6ee7e9ac5de`.

Environment: macOS-15.7.4-arm64-arm-64bit; Python 3.12.9; DuckDB 1.5.5; 11 logical CPUs reported. Other local processes were not stopped.

## Method

30 serial measured before/after pairs per operation, after 3 warm-up pairs. Execution order alternates on every pair. No parallel requests, cache flushes, cold-start simulation, or network requests are made. All timings use a monotonic high-resolution clock.

Repository timings include connection setup, two SQL queries, row materialization, and pagination bookkeeping. Before: a new read-only connection per query. After: one request-local read-only connection for the count and rows. No global connection pool or result cache is introduced. Connections close before returning.

Profile timings use in-process ASGI requests, including current HTML rendering and middleware. Metadata/timeline caches are warmed, while activity pagination remains uncached. Request logging output is disabled only in this benchmark process. These are not browser-load, TCP, TLS, CDN, cold-start, or Vercel latency measurements.

Cases are selected deterministically by activity count and UUID: the upper-middle positive history, largest history (first and out-of-range/clamped-last page), a one-day Swim filter on the largest history, and a registry athlete with no activity records. No athlete IDs or response bodies are exported. Every pair must return identical repository objects and byte-identical HTML, otherwise the run fails.

## Measured operations

| Case | History / matched / returned records | Repository median before → after (ms) | Repository p95 before → after (ms) | Profile median before → after (ms) | Profile p95 before → after (ms) | HTML bytes before → after |
| --- | --- | --- | --- | --- | --- | --- |
| median_positive_history | 293 / 293 / 30 | 35.842 → 22.655 | 37.414 → 23.567 | 48.561 → 34.796 | 49.807 → 35.453 | 80735 → 80735 |
| largest_history_first_page | 1034 / 1034 / 30 | 41.654 → 28.43 | 42.564 → 28.823 | 54.512 → 40.771 | 55.457 → 41.303 | 82046 → 82046 |
| largest_history_last_page | 1034 / 1034 / 14 | 42.258 → 28.956 | 50.556 → 32.592 | 54.555 → 41.048 | 88.044 → 45.29 | 75598 → 75598 |
| largest_history_one_day_swim_filter | 1034 / 0 / 0 | 30.282 → 17.277 | 31.997 → 18.136 | 39.586 → 26.281 | 45.254 → 28.924 | 69043 → 69043 |
| no_record_history | 0 / 0 / 0 | 29.019 → 15.982 | 32.574 → 18.501 | 42.224 → 27.576 | 62.473 → 51.431 | 33438 → 33438 |

p95 is the nearest-rank sample percentile, not an uncertainty interval. Small local timing samples can vary across machines and runs; no CI timing threshold is imposed.

## Reproduce

```sh
.venv/bin/python scripts/benchmark_activity_serving.py --iterations 30 --warmups 3
```

Requires the full baseline commit above to exist in local Git history. The script executes that committed serving module as the baseline; use only trusted repository revisions. It does not fetch, check out, reset, or modify the worktree. Optional `--json` writes aggregate samples summaries and fingerprints; no athlete-level export is created.

Production and the packaged database were not modified. Payload size is unchanged by design. Hosted staging load, cold starts, and browser performance remain separate P1 work.
