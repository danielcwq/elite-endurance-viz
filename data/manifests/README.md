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

Reconcile an export against the legacy activity CSV with:

```bash
.venv/bin/python scripts/snapshot_2024.py reconcile-activities \
  --mongo-activities data/raw/2024/mongodb/<snapshot>/activities.jsonl.gz
```

Generated manifest and report paths are immutable by default; commands refuse to overwrite them.
