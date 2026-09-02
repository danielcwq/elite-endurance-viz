# Source reconciliation status — 2026-09-02

## Repository sources

The repository CSV inventory completed successfully at commit `5ddf0bcab97acb177ae237363585f80ce58988f9`:

- 291 tracked CSVs;
- 1,371,610,676 bytes;
- 27 distinct header schemas;
- 0 parse failures after allowing the oversized raw-JSON fields present in 22 files.

See [`repository_csv_snapshot_2026-09-02.csv`](repository_csv_snapshot_2026-09-02.csv) for file-level checksums and [`repository_csv_snapshot_2026-09-02.json`](repository_csv_snapshot_2026-09-02.json) for the summary.

The current legacy activity candidate contains 147,070 rows, 137,478 unique activity IDs, and 9,592 duplicate extras. Its UTC start timestamps include 12 rows from 2023, 147,033 from 2024, and 25 from 2025. See [`local_activity_audit_2026-09-02.json`](local_activity_audit_2026-09-02.json).

## MongoDB sources

The export command was attempted against the configured Atlas cluster on 2026-09-02. Atlas did not present a consistently usable connection: attempts encountered no selectable primary and network/TLS timeouts. A longer retry reached the activities cursor and transferred roughly 4,700 documents before Atlas raised `NetworkTimeout`. The exporter removed the incomplete temporary payload. No partial export was retained.

The following figures are historical audit observations from 2026-08-31, not a current immutable export:

- 150,620 activity documents;
- 139,949 unique activity IDs;
- 10,671 duplicate extras;
- 3,550 activity rows observed in production but absent from the repository activity CSV.

These figures must not be treated as reconciled until a successful export is manifested.

## Unblocking sequence

1. Confirm that the Atlas cluster is running and has a primary, and that the current IP/network is allowed.
2. Run `.venv/bin/python scripts/snapshot_2024.py export-mongo`.
3. Copy the resulting timestamped raw directory to durable storage and verify it against its committed SHA-256 manifest.
4. Run `.venv/bin/python scripts/snapshot_2024.py reconcile-activities --mongo-activities <activities.jsonl.gz>`.
5. Review repository-only and Mongo-only ID reports before promoting either source into staging.

The exporter refuses to overwrite an existing snapshot, removes incomplete temporary exports, and makes a completed snapshot read-only.
