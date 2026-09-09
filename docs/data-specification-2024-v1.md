# EnduranceViz 2024 dataset specification

Specification version: **1.0.0**

Machine-readable contract: [`config/snapshot_2024.yaml`](../config/snapshot_2024.yaml)

## Purpose and limits

This release is a frozen analytical snapshot of publicly observable elite-athlete training and performance data. Strava activity visibility, athlete posting behavior, scraper success, and account discovery all affect coverage. The data must not be described as a complete training log or used to infer that no recorded activity means no training occurred.

The snapshot is designed to support a reproducible retrospective study of 2024. It is not a live feed. Matching the cohort to 2025 outcomes and developing prediction models are outside this version's scope.

## Canonical time window

The snapshot includes records at or after `2024-01-01T00:00:00Z` and before `2025-01-01T00:00:00Z`.

An activity is assigned by its start timestamp in UTC. An activity that crosses midnight, a week boundary, or year-end remains assigned to the day, week, and year in which it started. This makes inclusion deterministic and avoids splitting a source activity.

Weeks begin Monday at `00:00:00 UTC`. Week identifiers must use the UTC Monday date (`week_start_utc`) rather than only a week number. The final bucket beginning `2024-12-30` is retained for annual totals but is not a complete seven-day analytical week inside the snapshot.

## Canonical units

Curated tables use:

- meters for distance;
- seconds for elapsed, moving, and activity duration;
- UTC timestamps in ISO 8601 form;
- seconds per kilometer for pace when pace is materialized.

Source values and source units remain preserved in raw records. Conversion is a staging operation, never an in-place raw-data edit.

## Activity categories

The curated category is deliberately small while the original Strava type is retained:

| Category | Raw Strava types |
| --- | --- |
| `Run` | `Run`, `TrailRun`, `VirtualRun` |
| `Ride` | `Ride`, `VirtualRide`, `MountainBikeRide`, `EMountainBikeRide`, `EBikeRide`, `GravelRide` |
| `Swim` | `Swim` |
| `Other` | Every other non-null type |

Unknown or null raw types are quarantined until their treatment is explicit. Adding a new mapped type requires a specification-version change and a rebuilt manifest.

## Coverage vocabulary

Coverage is defined per athlete and Monday-based UTC week:

- **Observed week:** a successful athlete-week collection record exists, whether or not an activity was returned.
- **Active week:** at least one included public activity starts during the athlete-week.
- **Missing week:** no successful collection record exists. Missing is not zero.
- **Complete-enough week:** the week is observed, spans seven days entirely within the snapshot, was collected after the week ended, and has no collection or parsing error.

An activity proves that at least some data was visible in a week, but activity rows alone do not prove that the entire week was observed. The legacy `2024 Weeks Scraped` aggregate does not identify which weeks succeeded and is therefore insufficient by itself for week-level coverage. Until collection evidence is reconstructed, affected coverage values must remain `unknown`.

## Source and transformation layers

The filesystem contract is:

```text
data/
├── raw/2024/       immutable source exports
├── staging/        typed and normalized intermediate data
├── curated/2024/   canonical analytical tables
├── derived/2024/   aggregates, charts, and study outputs
├── manifests/      committed checksums and reconciliation reports
└── quarantine/2024/ records excluded with explicit reason codes
```

Raw payloads are write-once. Cleaning code reads raw files and writes to staging or later layers. The existing tracked CSVs are preserved by Git plus the repository CSV manifest; they are not duplicated into `data/raw/2024`. The repository-missing activity rows recovered from Mongo are stored as a separate, immutable reconciliation CSV so the legacy activity file remains untouched and every build uses explicit source provenance.

MongoDB exports are stored locally under a timestamped `data/raw/2024/mongodb/` directory and are ignored by Git. Their small manifests are committed. A completed export is made read-only and copied to redundant offline storage using its manifest checksums. The small, manifested production-only reconciliation CSV is committed because it is required for credential-free rebuilding.

## Versioning rules

The specification follows semantic versioning:

- patch: documentation clarification that does not change rows or values;
- minor: backward-compatible columns or derived metrics;
- major: inclusion, identity, unit, category, coverage, or conflict-resolution changes that can change canonical observations.

Every curated release must cite the specification version, source manifests, build commit, and build timestamp.

## Database position

PostgreSQL is not the source of truth for the frozen analytical release. Immutable raw exports, checksums, and reproducible curated files are the source of truth. DuckDB/Parquet is the preferred first analytical representation because it is portable and easy to rebuild. PostgreSQL may later serve the web application when concurrent queries, indexes, and operational migrations justify it; it should be populated from the canonical build rather than edited independently.
