# Data layout

The canonical 2024 layout and definitions are documented in [`docs/data-specification-2024-v1.md`](../docs/data-specification-2024-v1.md).

- `raw/2024`: immutable source snapshots; generated Mongo payloads are intentionally ignored by Git.
- `staging`: typed intermediate outputs that may be rebuilt.
- `curated/2024`: canonical analytical tables that may be rebuilt.
- `derived/2024`: aggregates and study outputs that may be rebuilt.
- `manifests`: committed inventories, checksums, audits, and reconciliation reports.
- `quarantine/2024`: excluded records and reason codes that may be rebuilt.

The legacy `raw_data`, `tempdata`, and `metadata/backup` directories remain untouched. Their contents are cataloged by the repository CSV manifest and will be migrated only through reproducible transformations.

## Authority

The manifested repository CSV snapshot is the currently rebuildable source release. `data/reference/athlete_registry_2024.csv`, `data/reference/athlete_external_accounts_2024.csv`, configuration under `config/`, and schema under `schema/` are versioned control inputs.

Generated Parquet and DuckDB files are authoritative only for the code/configuration version that built them and remain ignored because they are reproducible. Committed JSON manifests and quality reports record their counts and checks. The application reads `data/derived/2024/enduranceviz_2024.duckdb`, produced atomically by `scripts/pipeline_2024.py serve`.

Legacy Mongo collections, `cleaned_athlete_metadata.csv` aggregate columns, `data/metadata/athlete_statistics.csv`, notebooks, raw batch summaries, and timestamped backups are provenance-only. Analytical queries must use canonical performance, activity, coverage, weekly, and athlete-summary tables instead.
