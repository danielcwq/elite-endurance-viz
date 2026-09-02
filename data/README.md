# Data layout

The canonical 2024 layout and definitions are documented in [`docs/data-specification-2024-v1.md`](../docs/data-specification-2024-v1.md).

- `raw/2024`: immutable source snapshots; generated Mongo payloads are intentionally ignored by Git.
- `staging`: typed intermediate outputs that may be rebuilt.
- `curated/2024`: canonical analytical tables that may be rebuilt.
- `derived/2024`: aggregates and study outputs that may be rebuilt.
- `manifests`: committed inventories, checksums, audits, and reconciliation reports.
- `quarantine/2024`: excluded records and reason codes that may be rebuilt.

The legacy `raw_data`, `tempdata`, and `metadata/backup` directories remain untouched. Their contents are cataloged by the repository CSV manifest and will be migrated only through reproducible transformations.
