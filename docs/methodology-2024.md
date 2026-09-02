# EnduranceViz 2024 methods, lineage, and limitations

## Source-to-serving lineage

```text
tracked repository CSVs ── checksums ──> immutable source manifest
sealed Mongo snapshot ── ID reconciliation ──> production-only activity supplement
          │
          ├── World Athletics performance rows ──> performances_2024
          │                                           │
          ├── cleaned metadata + activity evidence ──> athletes
          │                                           │
          │                                    external accounts
          │                                           │
          ├── legacy + supplemental activity rows ── deduplicate/validate ──> activities_2024
          │                                                    │
          └── weekly scrape rows ── coverage evidence ──> data_coverage_2024
                                                               │
activities_2024 + data_coverage_2024 ──> weekly_training_2024 ──┤
                                                               v
                                                    athlete_summary_2024
                                                               │
                                           constraints + quality assertions
                                                               │
                                                               v
                                             indexed read-only DuckDB app
```

Raw weekly totals never flow into analytical totals. They establish only whether an athlete-week was observed. All distances, durations, activity counts, and summaries are recalculated from canonical deduplicated activities.

## Identity rules

`athlete_id` is a random UUID persisted in `data/reference/athlete_registry_2024.csv`. It is independent of names and provider accounts. Names are normalized for matching only; official and Strava display names remain separate attributes.

External identities are keyed by `(provider, external_account_id)`. Activity or cleaned-metadata evidence takes precedence over a weekly display-name alias. Weekly-only accounts can extend the mapping only when their normalized name resolves to exactly one canonical athlete. Ambiguous or unmatched source rows must be quarantined, never assigned by file order or activity volume.

The current registry resolves 678 Strava accounts, including 216 supported only by weekly collection evidence. Jack Rayner and Mario García Romo's zero legacy IDs and Kevin Robertson and Wes Ferguson's additional accounts are explicitly resolved. Six unauthoritative multi-ID values from performance-processing fields are excluded because no canonical activity or metadata evidence corroborates them.

## Activity deduplication and validation

Strava Activity ID is the canonical activity key. Exact duplicates and equivalent duplicates within 20 meters or 0.5% are reduced to the most complete, precise source row. Core conflicts in athlete, timestamp, type, elapsed duration, or out-of-tolerance distance quarantine the whole duplicate group.

Activities are assigned to the UTC year and Monday UTC week of their start timestamp. Distances are meters, durations seconds, and pace seconds per kilometer. Legacy swim distance is null because the scraper discarded source units. Invalid timestamps, window boundaries, impossible distance/duration, and moving-time violations are quarantined with source JSON and reason codes.

## Coverage model

- Observed: a weekly collection record exists, including explicit `No Data`.
- Active: at least one canonical activity starts in the week.
- Missing: an athlete has a resolved provider account but no collection evidence for the week.
- Unknown: no provider account or weekly collection evidence establishes observability.
- Complete-enough: an observed seven-day week has no evidence conflict or processing warning.

Coverage warnings detect mismatched requested/date ranges, conflicting duplicate summary rows, and contradictions between `No Data` summaries and canonical activity presence. The partial week starting December 30 is retained for annual totals but excluded from full-week coverage and rolling comparisons.

Coverage score is 70% observed breadth (`observed full weeks / 52`) plus 30% evidence consistency (`complete-enough / observed`). The audited status thresholds are high at 90, moderate at 75, low at 50, and insufficient below 50; no evidence is unknown. High and moderate athletes (score at least 75) form the default 585-athlete cohort. Others remain discoverable.

## Metric definitions

| Metric | Definition |
| --- | --- |
| Effective duration | Moving seconds when available; otherwise elapsed seconds |
| Observed-week average | Sum across observed full weeks divided by observed full-week count |
| Calendar-week average | Full-snapshot total divided by `366 / 7` week-equivalents |
| Long-run share | Longest run distance divided by total run distance in that week |
| Cross-training share | Non-run effective duration divided by all effective training duration |
| Double-session days | UTC dates containing at least two unique public activities |
| Four-week average | Four-week run-distance sum divided by four, only when all four weeks are complete-enough |
| Week-over-week change | Difference between adjacent complete-enough weeks; fraction is null if the prior week is zero |
| Weekly consistency | `1 / (1 + population coefficient of variation)` across observed weeks |
| Weighted run pace | Effective run duration divided by canonical run distance |

Every public athlete summary carries the half-open window, observed-week denominator, dataset name/version, build time, and coverage status. Season bests are queried from one-row-per-performance data; the legacy pipe-delimited `Mark` and `Discipline` fields are never zipped or served.

## Known limitations

- Public Strava posts are a selective view of training. Private, hidden, deleted, manually omitted, or differently categorized work is absent.
- Coverage evidence proves collection attempts, not completeness of an athlete's real training log.
- UTC week assignment gives a consistent cross-athlete boundary but can differ from Strava's athlete-local week near midnight. Strava's weekly progress uses Monday through Sunday in the athlete's local calendar.
- Moving time is missing for some activities and falls back to elapsed time; pace comparisons therefore have mixed source semantics and are labeled accordingly.
- Swim distance is unavailable until unit-preserving raw payloads are recovered.
- Strength and cross-training categories depend on public Strava activity types and do not measure unposted gym work.
- The 1,100 World Athletics result-score threshold shaped source selection and is not a universal definition of elite status.
- Observational comparisons cannot establish causality or prescribe training.
- The Mongo snapshot is a one-time historical export, not a current feed. Its 3,550 repository-missing rows represent 2,471 unique activity IDs from November 4 through December 30, 2024; 2,469 pass canonical validation and two remain quarantined. No post-snapshot production changes are represented.

## Authoritative versus provenance-only files

Authoritative inputs are the manifested tracked CSV snapshot, the manifested Mongo reconciliation supplement, the persistent reference registries, the snapshot contract, discipline mapping, and identity overrides. The complete Mongo payload is retained in redundant offline storage and verified by `mongodb_snapshot_20260902T190000Z.csv`; it is not queried by the application. Authoritative generated tables are the current Parquet/DuckDB outputs rebuilt from the committed inputs; committed audit JSON and the quality report describe them.

`Get_Data/*.ipynb`, `OLY24 Pred/`, `data/metadata/athlete_statistics.csv`, legacy weekly aggregate columns, Mongo helper/test scripts, timestamped backups, and raw/temp processing batches are provenance-only. They may explain history but must not be queried as current analytical truth.

## Reproducible commands

```bash
# Full build, validation, and atomic serving database
.venv/bin/python scripts/pipeline_2024.py build

# Assertions and human-readable report only
.venv/bin/python scripts/pipeline_2024.py validate

# Recreate the indexed DuckDB serving layer from canonical outputs
.venv/bin/python scripts/pipeline_2024.py serve

# Unit and transformation tests
.venv/bin/python -m unittest discover -s tests -v
```

The build uses paths resolved from the repository rather than the caller's working directory. Re-running it replaces generated tables and the serving database atomically without appending duplicate records.
