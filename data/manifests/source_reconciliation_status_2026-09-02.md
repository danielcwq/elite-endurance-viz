# Source reconciliation status — 2026-09-02

## Outcome

Source reconciliation is complete. The canonical 2024 build combines the untouched legacy repository activity CSV with a separate immutable supplement recovered from the sealed production snapshot.

- Full Mongo snapshot: `data/raw/2024/mongodb/20260902T190000Z` (ignored by Git, read-only, and copied to redundant external storage).
- Snapshot manifest: [`mongodb_snapshot_20260902T190000Z.csv`](mongodb_snapshot_20260902T190000Z.csv).
- Activity reconciliation: [`activity_reconciliation_20260902T191500Z.json`](activity_reconciliation_20260902T191500Z.json).
- Rebuildable supplement: [`production_only_activities.csv`](../raw/2024/reconciled/20260902T190000Z/production_only_activities.csv).
- Supplement manifest: [`mongodb_activity_supplement_20260902T190000Z.csv`](mongodb_activity_supplement_20260902T190000Z.csv).

Checksums for every copied full-snapshot payload matched the committed manifest.

## Repository and production comparison

The original repository activity source contains 147,070 rows, 137,478 unique activity IDs, and 9,592 duplicate extras. Production contains 150,620 rows, 139,949 unique IDs, and 10,671 duplicate extras.

The ID reconciliation is exact:

- 137,478 IDs occur in both sources;
- zero IDs occur only in the repository;
- 2,471 IDs occur only in production;
- those production-only IDs account for 3,550 rows and 1,079 duplicate extras.

Every production-only row starts between `2024-11-04T03:12:34Z` and `2024-12-30T00:36:04Z`. All 43 associated Strava accounts already resolve to persistent athlete UUIDs. No duplicate group has a core conflict. After the standard validation rules, 2,469 recovered unique activities enter `activities_2024`; two are quarantined for invalid or inconsistent duration. The resulting canonical table contains 139,887 activities.

## Mongo snapshot

All four expected collections were exported:

| Collection | Documents | Compressed bytes |
| --- | ---: | ---: |
| `activities` | 150,620 | 9,767,745 |
| `athlete_metadata` | 460 | 31,978 |
| `master_iaaf` | 5,305 | 257,891 |
| `update_logs` | 2 | 222 |

Initial attempts with one long-lived sorted Atlas cursor suffered repeated network timeouts. The final exporter first inventories `_id`, then retrieves deterministic 1,000-document partitions and verifies that each requested ID is returned exactly once. This completed without partial payloads and preserves every document, including activity duplicates.

## External-drive audit

The mounted offline backup was scanned read-only before using Atlas. The committed [`external-drive audit`](external_drive_audit_2026-09-02.json) and [`CSV manifest`](external_drive_csv_snapshot_2026-09-02.csv) cover 598 candidate CSV files, 209 unique hashes, and 27 schemas.

The drive contains February 2025 repository/data backups rather than a Mongo/BSON/JSONL collection export. Its raw JSON activity evidence is a subset of the current repository evidence. The only 12 drive-only activity IDs are January–February 2025 Thomas Bridger activities, outside the canonical 2024 window. Three backup CSVs contain 19 row-width mismatches, including a headerless/glued tail; none contributes a repository-missing 2024 ID.

## Reproduction

The full payload is not required for ordinary builds. The small reconciled supplement is committed, manifested, and configured in `config/snapshot_2024.yaml`, so a new developer can rebuild without Mongo credentials:

```bash
.venv/bin/python scripts/pipeline_2024.py build
```

To repeat the source procedure from a newly exported payload:

```bash
.venv/bin/python scripts/snapshot_2024.py reconcile-activities \
  --mongo-activities data/raw/2024/mongodb/<snapshot>/activities.jsonl.gz

.venv/bin/python scripts/snapshot_2024.py extract-mongo-activity-supplement \
  --mongo-activities data/raw/2024/mongodb/<snapshot>/activities.jsonl.gz
```

Both snapshot and supplement writers refuse to overwrite immutable outputs.
