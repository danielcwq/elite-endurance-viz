# Snapshot manifests

Manifests are small, committed evidence about immutable or externally stored source payloads. They contain checksums, row counts, schemas, extraction timestamps, and source systems.

Generate the repository CSV inventory with:

```bash
.venv/bin/python scripts/snapshot_2024.py inventory-repository
```

Export the four live MongoDB collections with:

```bash
.venv/bin/python scripts/snapshot_2024.py export-mongo
```

The exporter uses bounded `_id` partitions so an unstable long-lived Atlas cursor cannot leave a silently incomplete snapshot. Completed payloads are write-once and read-only.

Reconcile an export against the legacy activity CSV with:

```bash
.venv/bin/python scripts/snapshot_2024.py reconcile-activities \
  --mongo-activities data/raw/2024/mongodb/<snapshot>/activities.jsonl.gz
```

Extract the reconciled Mongo-only rows needed by the credential-free build with:

```bash
.venv/bin/python scripts/snapshot_2024.py extract-mongo-activity-supplement \
  --mongo-activities data/raw/2024/mongodb/<snapshot>/activities.jsonl.gz
```

Audit an offline backup without modifying it with:

```bash
.venv/bin/python scripts/snapshot_2024.py audit-external-drive \
  --source-root /path/to/elite-endurance-viz-backups
```

Generated manifest and report paths are immutable by default; commands refuse to overwrite them.
