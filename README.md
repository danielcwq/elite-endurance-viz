# EnduranceViz: the 2024 snapshot

EnduranceViz is a reproducible study of publicly observable 2024 training among elite endurance runners. It combines public World Athletics results with public Strava activities, resolves both to stable internal athlete IDs, and serves coverage-aware metrics from a canonical analytical model.

This is a fixed historical snapshot—not a live training tracker and not a claim to represent anyone's complete training history.

## Current dataset

- Window: `2024-01-01T00:00:00Z` through, but excluding, `2025-01-01T00:00:00Z`
- Athletes: 3,609 World Athletics identities
- Resolved Strava accounts: 678
- Canonical performances: 5,305
- Canonical activities: 139,887 after removing 10,671 duplicate extras and quarantining 62 invalid/window rows
- Legacy P0 coverage flag: 585 athletes (499 high, 86 moderate); this is not the approved P1 analytical inclusion rule
- Canonical engine: Parquet plus DuckDB
- Dataset specification: `1.0.0`

The live Mongo collections, notebooks, and CSV summary columns are not the analytical source of truth. A sealed Mongo export was reconciled once; its 3,550 repository-missing rows are retained as a manifested supplemental input, while the application serves only rebuilt canonical tables.

## Start locally and run checks

Install Python 3.12 or `uv`, then run:

```bash
make setup
make run
```

The site opens at `http://127.0.0.1:8000`. Setup installs the web, pipeline, and optional figure dependencies into `.venv`; it does not rebuild the dataset or require database credentials. An existing valid Python 3.12 environment is reused, not deleted. Without Make, use `python3 scripts/dev.py setup` and `python3 scripts/dev.py run`.

```bash
make test   # Unit, synthetic-fixture, schema, HTTP, and deployment-contract tests
make check  # Those tests plus 19 read-only checks against the packaged snapshot
```

The packaged-data check reuses the 16 P0 quality assertions and independently reconciles all P1 athlete-week record counts, UTC running days, and measurement-complete running distances. P0 coverage checks validate the released artifact's historical contract, not the validity of a P1 inclusion threshold. Neither command rebuilds or replaces any database.

GitHub Actions runs the same tests and packaged-data checks on P1/main pushes and pull requests. Its token is read-only; it has no deployment or external-database steps. See [developer checks and CI](docs/p1-developer-checks.md) for scope and limitations.

On the P1 branch, `/health` probes the snapshot read-only and returns 200/503. Structured request logs include route patterns, status, duration, and request IDs, without raw request URLs or athlete data. `make run` disables the separate Uvicorn access log. See [operational observability](docs/p1-operational-observability.md) for the failure behavior and infrastructure-log boundary.

## Rebuild the P0 snapshot locally

Python 3.12 is recommended.

```bash
uv venv --python 3.12
uv pip install --python .venv/bin/python -r requirements-pipeline.txt
.venv/bin/python scripts/pipeline_2024.py build
.venv/bin/python scripts/package_serving_artifact.py
```

Standard `python3 -m venv .venv` plus `.venv/bin/python -m pip install -r requirements-pipeline.txt` is equivalent when the Python installation bundles `pip`. `requirements.txt` deliberately contains only web-runtime dependencies so Vercel does not package the analytics toolchain.

That one command rebuilds curated observations, coverage-aware weekly and athlete tables, a quality report, and the atomic local serving database at `data/derived/2024/enduranceviz_2024.duckdb`. It needs no production, MongoDB, World Athletics, or Strava credentials because the repository inputs are already snapshotted and manifested.

Run individual stages when iterating:

```bash
.venv/bin/python scripts/pipeline_2024.py validate
.venv/bin/python scripts/pipeline_2024.py serve
.venv/bin/python main.py
```

The application queries DuckDB read-only. It prefers the locally rebuilt file under `data/derived/2024/`, then falls back to the checksummed copy under `deploy/`. Set `ENDURANCEVIZ_DB_PATH` to use an equivalent built artifact elsewhere.

The `data/` deployment guardrail remains intact: `.vercelignore` excludes the entire raw, staged, curated, and derived tree. Only the immutable serving database and its checksum manifest are allowlisted into the Vercel function from `deploy/`.

## Data model and methods

- [Runtime architecture, data lineage, and schema diagrams](docs/architecture.md)
- [2024 dataset specification](docs/data-specification-2024-v1.md)
- [Data dictionary](docs/data-dictionary-2024.md)
- [Methods, lineage, and limitations](docs/methodology-2024.md)
- [Example passing quality report](docs/example-data-quality-report-2024.md)
- [P0/P1 roadmap](TODO.md)

## Public application behavior

Athletes are routed and joined by persistent UUID, not by display name. Search is server-side, activity pages retrieve at most 30 projected rows at a time, and public counts come from canonical deduplicated tables.

On the P1 branch, profiles display recorded-week distance, Run-record counts, and UTC running days with explicit denominators, gaps, source warnings, and snapshot version/build date. Date/category controls filter only the activity table. These replace the legacy P0 profile summary cards and coverage-score headline; the packaged P0 database itself remains unchanged. This describes branch behavior, not a claim that P1 is deployed.

The approved exploratory study leads with 800m versus 5000m, uses separate recorded-sex panels, and gives each athlete one median summary over their recorded-running weeks. No annual-week cutoff, final inclusion rule, performance tier, or inferential claim has been approved. See [analysis decisions](docs/p1-analysis-decisions.md) and the [exploratory report](docs/p1-exploratory-training-2024.md).

## Historical work

`OLY24 Pred/`, `README-OLY.md`, and the notebooks under `Get_Data/` preserve the original Olympic prediction and data-collection research. They are useful precedent, but none is required to rebuild or serve the 2024 snapshot.
