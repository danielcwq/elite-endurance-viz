# EnduranceViz 2024 data dictionary

This dictionary describes the canonical DuckDB/Parquet model declared in [`schema/2024.sql`](../schema/2024.sql). Raw source headers remain unchanged in manifested inputs; these names and types apply only after staging and curation.

## Model relationships

```text
dataset_builds ──< import_manifest
       │
       └────────< quarantined_records

athletes ──< athlete_external_accounts
    │
    ├──────< performances_2024
    ├──────< activities_2024
    ├──────< data_coverage_2024 ──1 weekly_training_2024
    └──────1 athlete_summary_2024
```

`athlete_id` is an internal UUID persisted in the identity registry. It is not a Strava identifier and must not be regenerated from an athlete's current display name.

## Provenance tables

### `dataset_builds`

One row per attempted canonical build. `build_id` is the lineage key used by manifests and quarantined records. A successful release has `status = 'succeeded'` and a non-null completion timestamp.

### `import_manifest`

One row per physical source file used by a build. It records SHA-256, byte and row counts, schema signature, extraction time, source system, source role, and source commit. `(build_id, source_file, sha256)` is unique.

### `quarantined_records`

Records excluded from canonical tables, including their original JSON representation, source row, stable reason code, explanation, and build. Quarantine is evidence, not deletion.

## Identity tables

### `athletes`

One row per real athlete. The internal UUID is the primary key. Official and display names are attributes, not identifiers. `identity_status` distinguishes resolved, ambiguous, and unmatched records. Nationality is a nullable three-letter source code; it is not assumed to represent citizenship at all times.

### `athlete_external_accounts`

One row per provider account. `(provider, external_account_id)` is the primary key and therefore prevents one Strava account from mapping to multiple athletes. Matching method and status preserve how the link was established.

## Observation tables

### `performances_2024`

One row per public World Athletics performance in 2024. The source mark is always retained as `mark_text`; parsed time and distance marks occupy separate nullable columns. The natural performance fields are unique within an athlete. Dates are strongly typed and constrained to 2024.

`is_season_best` identifies the minimum parsed time for an athlete and discipline, retaining tied bests. Primary discipline is selected by highest 2024 World Athletics result score, then the most performances in that discipline, then canonical discipline name ascending. `is_primary_discipline` is true on all of the athlete's rows in that discipline.

### `activities_2024`

One row per unique Strava activity starting in the canonical 2024 UTC window. The source Strava activity ID is the primary key. Source activity type is preserved and a normalized four-value category is added.

Canonical measurements are meters, seconds, UTC timestamps, and seconds per kilometer. Negative measurements are rejected. `moving_seconds` remains null where the legacy source does not distinguish it from elapsed or activity time.

Legacy swim distances are null in the first canonical release. The scraper discarded each displayed unit before storing its numeric value, so values may represent meters, yards, miles, or already-converted kilometers. Swim duration remains usable; distance can be restored only from raw payloads that preserve units.

`quality_status` is `valid` or `warning`; `quality_flags` names retained limitations such as missing moving time or suppressed swim units. Excluded observations live in `quarantined_records` with `exclusion_reason`, so a curated row normally has a null exclusion reason.

`week_start_utc` is the Monday date containing `start_at_utc`. This intentionally gives every athlete one comparison boundary; it does not claim to reproduce Strava's athlete-local calendar near midnight.

### `data_coverage_2024`

One row per athlete and UTC week, including the partial week beginning December 30. Observation status is `observed`, `missing`, or `unknown`; missing collection evidence must never become a numeric zero. Activity presence determines `is_active_week`. `unique_activity_count` is recalculated from canonical activities. Coverage warnings identify conflicting source rows, requested-week/date-range mismatches, and weekly-summary/activity contradictions.

The 2024 raw and temporary weekly summary CSVs are used only as evidence that collection was attempted. Their distance and time totals are never used as analytical measurements. When several rows exist for one account-week, a dated temporary recovery file wins over a raw batch row and the conflict remains flagged.

### `weekly_training_2024`

One row per athlete and UTC week, joined one-to-one with coverage. It stores unique activity counts, active and double-session days, longest-run share, sport-specific duration, strength and other cross-training, total duration, and cross-training share. Long-run share is the longest run divided by the week's total run distance. Swim distance remains null while legacy source units are ambiguous.

An observed inactive week has numeric zeros. An inactive missing or unknown week has null metrics. A week containing canonical activities retains those known metrics even if its collection coverage is missing. Four-week metrics require four consecutive `complete-enough` full weeks; week-over-week changes require both adjacent weeks to be complete-enough, and fractional change is null when the prior week is zero.

### `athlete_summary_2024`

One row per athlete derived from canonical weekly and activity data. It includes first/last activity timestamps, explicit observed-week and 366/7 calendar-week averages, variation, consistency, long-run, active-day, cross-training, and weighted pace measures.

Coverage score is 70% observed breadth (`observed full weeks / 52`) and 30% evidence consistency (`complete-enough / observed`). The audited thresholds are high at 90, moderate at 75, low at 50, and insufficient below 50; no collection evidence is `unknown`. The default comparison cohort includes high and moderate coverage (585 athletes in the current snapshot). Low-coverage and unknown athletes remain queryable.

The weighted run pace uses effective duration (moving time when available, otherwise elapsed time) divided by canonical run distance. Every summary carries the half-open metric window, dataset name/version, coverage status, and an explicit weekly denominator description.

## Storage contract

DuckDB is the executable constraint and validation engine. Each canonical table is also exported to Parquet for portable analytical use. Generated databases and Parquet files live under `data/curated/2024/` and are reproducible, so they are ignored by Git. PostgreSQL, if introduced for serving, must be loaded from these canonical outputs rather than becoming an independently edited data source.
