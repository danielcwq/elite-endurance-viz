# EnduranceViz: the 2024 snapshot

EnduranceViz is a reproducible study of publicly observable 2024 training among elite endurance runners. It combines public World Athletics results with public Strava activities, resolves both to stable internal athlete IDs, and serves coverage-aware metrics from a canonical analytical model.

This is a fixed historical snapshot—not a live training tracker and not a claim to represent anyone's complete training history.

## Current dataset

- Window: `2024-01-01T00:00:00Z` through, but excluding, `2025-01-01T00:00:00Z`
- Athletes: 3,609 World Athletics identities
- Resolved Strava accounts: 678
- Canonical performances: 5,305
- Canonical activities: 137,418 after removing 9,592 duplicate extras and quarantining invalid rows
- Default coverage-qualified cohort: 585 athletes (501 high, 84 moderate)
- Canonical engine: Parquet plus DuckDB
- Dataset specification: `1.0.0`

The old Mongo collections, notebooks, and CSV summary columns are not the analytical source of truth. They remain provenance artifacts.

## Rebuild locally

Python 3.12 is recommended.

```bash
uv venv --python 3.12
uv pip install --python .venv/bin/python -r requirements.txt
.venv/bin/python scripts/pipeline_2024.py build
```

Standard `python3 -m venv .venv` plus `.venv/bin/python -m pip install -r requirements.txt` is equivalent when the Python installation bundles `pip`.

That one command rebuilds curated observations, coverage-aware weekly and athlete tables, a quality report, and the atomic local serving database at `data/derived/2024/enduranceviz_2024.duckdb`. It needs no production, MongoDB, World Athletics, or Strava credentials because the repository inputs are already snapshotted and manifested.

Run individual stages when iterating:

```bash
.venv/bin/python scripts/pipeline_2024.py validate
.venv/bin/python scripts/pipeline_2024.py serve
.venv/bin/python main.py
```

The application queries the local DuckDB file read-only. Set `ENDURANCEVIZ_DB_PATH` to use an equivalent built artifact elsewhere. A deployment must build or supply that artifact before importing `main.py`.

## Data model and methods

- [2024 dataset specification](docs/data-specification-2024-v1.md)
- [Data dictionary](docs/data-dictionary-2024.md)
- [Methods, lineage, and limitations](docs/methodology-2024.md)
- [Example passing quality report](docs/example-data-quality-report-2024.md)
- [P0/P1 roadmap](TODO.md)

## Public application behavior

Athletes are routed and joined by persistent UUID, not by display name. Search is server-side, activity pages retrieve at most 30 projected rows at a time, and public counts come from canonical deduplicated tables. Every profile displays snapshot version, build date, coverage score/status, and explicit observed-week versus calendar-week metrics.

## Historical work

`OLY24 Pred/`, `README-OLY.md`, and the notebooks under `Get_Data/` preserve the original Olympic prediction and data-collection research. They are useful precedent, but none is required to rebuild or serve the 2024 snapshot.
