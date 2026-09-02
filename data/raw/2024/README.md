# Raw 2024 snapshots

This directory is write-once source storage. Do not clean or edit files here.

MongoDB exports are created in timestamped, read-only subdirectories by:

```bash
.venv/bin/python scripts/snapshot_2024.py export-mongo
```

Payloads are ignored by Git. Commit the generated manifest under `data/manifests/` and copy the payload directory to durable versioned storage before treating an export as safely preserved.
