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

### `activities_2024`

One row per unique Strava activity starting in the canonical 2024 UTC window. The source Strava activity ID is the primary key. Source activity type is preserved and a normalized four-value category is added.

Canonical measurements are meters, seconds, UTC timestamps, and seconds per kilometer. Negative measurements are rejected. `moving_seconds` remains null where the legacy source does not distinguish it from elapsed or activity time.

`week_start_utc` is the Monday date containing `start_at_utc`. This intentionally gives every athlete one comparison boundary; it does not claim to reproduce Strava's athlete-local calendar near midnight.

### `data_coverage_2024`

One row per athlete and UTC week. Observation status is `observed`, `missing`, or `unknown`; missing collection evidence must never become a numeric zero. Activity presence determines `is_active_week`, while successful post-week collection without errors determines completeness.

### `weekly_training_2024`

One row per athlete and UTC week, joined one-to-one with coverage. It stores activity counts, active days, and sport-specific distances and durations. Zero-valued totals are valid only alongside explicit observation status; consumers must not silently coalesce missing coverage to observed inactivity.

### `athlete_summary_2024`

One row per athlete derived from canonical weekly data. Weekly averages use the documented observed-week denominator. Coverage counts and status travel with all headline totals so the application can communicate uncertainty.

## Storage contract

DuckDB is the executable constraint and validation engine. Each canonical table is also exported to Parquet for portable analytical use. Generated databases and Parquet files live under `data/curated/2024/` and are reproducible, so they are ignored by Git. PostgreSQL, if introduced for serving, must be loaded from these canonical outputs rather than becoming an independently edited data source.
