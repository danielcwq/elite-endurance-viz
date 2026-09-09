# Deployment artifact

This directory contains the single immutable DuckDB snapshot bundled with the web application. It is a generated serving artifact, not the analytical source of truth.

Rebuild and validate the canonical data first, then refresh this directory with:

```bash
.venv/bin/python scripts/package_serving_artifact.py
```

`serving-artifact.json` records the database size, SHA-256 digest, dataset version, build timestamp, and source commit. The Vercel configuration explicitly includes only these deployment files; `.vercelignore` continues to exclude the entire `data/` tree.
