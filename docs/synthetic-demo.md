# Synthetic developer demo

This is invented software-test data, not a sample or anonymized derivative of the
2024 athlete snapshot. It supplies a small dataset for local demos, onboarding,
and regression tests without opening any real athlete data files.

## Run

After `make setup`, run `make demo` and open `http://127.0.0.1:8001`.
The command creates `data/derived/2024/demo/synthetic.duckdb` if absent, verifies
its synthetic version marker, and launches the same FastHTML app with that
database in the child process environment. It does not change your shell
environment or an existing app process. Stop the demo with Ctrl-C.

The existing `make run` still uses port 8000 and normal snapshot selection.
The demo banner says “Synthetic demo — not real athletes”; names and activity
titles also identify themselves as synthetic. The study layout is reused for
testing, not a study of these fabricated observations.

To build without starting the app:

```sh
make seed
# Or choose a new, nonexisting output path:
.venv/bin/python scripts/build_demo_2024.py --database /tmp/my-new-synthetic-demo.duckdb
```

`make seed` refuses an existing destination, including broken symlinks. `make demo`
reuses an existing demo with the expected version marker; it does not silently
rebuild after code changes. To regenerate, choose a new path or explicitly move
the old demo file aside after stopping its server. No force-overwrite flag exists.
Without Make, use `python3 scripts/dev.py demo` or `seed`.

## Contents

Five deterministic fictional identities, four placeholder accounts, four results,
55 activities, and 265 athlete-week rows exercise the real schema and queries.

| Synthetic athlete | Example to inspect |
| --- | --- |
| Frequent | Eight weeks of five Runs, 40 activities across two pages; a legacy ambiguous-empty week and later gaps. |
| Missing Distance | A warning week with one unavailable Run distance, a genuine zero-distance Run, and a partial December 30 week. |
| Cross Training | Ride, Swim, and strength records but no Run history; a missing performance score. |
| No Observations | No accounts, performances, nationality, or activities; recorded sex is unknown. |
| Split Efforts | Eight short records on one UTC date, with no weekly collection record. These must not become eight sessions. |

Search for `SYNTHETIC`. Invented UUIDs end in `000000000001` through `000000000005`.
Source IDs are deliberately nonnumeric `synthetic-*` placeholders, not valid
Strava references. External links in the reused UI are not real profiles or part
of this demo. CSS, map assets, and map tiles still use their normal public
endpoints; this is not an offline UI.

## Build and safety boundaries

- The authored definition is [synthetic SQL](../fixtures/synthetic_2024.sql), with no
  extracted names, locations, descriptions, account IDs, or observations.
- The [builder](../scripts/build_demo_2024.py) reads only the schema, synthetic SQL,
  configuration, and generator-code files. It does not read `data/`, `deploy/`, CSV
  exports, MongoDB, Strava, or World Athletics. Dependency installation is separate.
- All ten physical tables use [the production DDL](../schema/2024.sql), including
  foreign keys. P0 weekly/summary storage functions are reused; P1 computes its
  measurement-aware metrics on read. This does not approve P0 coverage thresholds
  as P1 analytical inclusion rules.
- [Serving indexes and the directory view](../enduranceviz/database.py) are shared
  with the canonical populate command, not maintained as a separate mock schema.
- The build checks activity/weekly/summary reconciliation and the three P1 calendar,
  record/day, and distance checks before publication. Real-snapshot count baselines
  intentionally do not apply to this five-athlete demo.
- Temporary construction and exclusive hard-link publication refuse existing files
  and concurrent destination creation. Failures leave no published database.
  Outputs under `deploy/` and the default canonical local path are rejected.
  The packaging command also rejects the synthetic version.
- Definitions are tracked; the generated database is Git-ignored under
  `data/derived/2024/`. The `data/` deployment exclusion remains intact.
- `SYNTHETIC-DEMO-v1`, fixed fixture timestamps, and input-code hashes establish
  provenance. Manifest `row_count=0` denotes generator code/configuration, not a
  tabular import. Relational contents are deterministic for the same definitions;
  byte-identical DuckDB files are not promised. The fixed timestamp is a fixture
  value, not the wall-clock time at which the demo was generated.

## Tests and limitations

`make test` includes fresh-process HTTP checks against the seed: health, homepage,
five profiles, search, map counts, pagination, and a category/date-filtered empty
result. Additional tests cover deterministic contents, metric edge cases, no real
data-file reads, overwrite/race protection, failure cleanup, and package rejection.

This seed supports onboarding and branch QA. It is not representative training,
a production-load benchmark, or a substitute for real-snapshot checks. The complete
test suite still has packaged-snapshot integration tests. For isolated demo tests:

```sh
.venv/bin/python -m unittest discover -s tests -p test_demo_dataset.py
```

Providing the seed does not remove historical data already tracked in this
repository or make a normal Git clone data-free.
