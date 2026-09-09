# Developer checks and continuous integration

## Entry points

From the repository root, `make setup` creates a Python 3.12 `.venv` only when absent and installs `requirements-analysis.txt`, including web/pipeline requirements. It uses an installed `uv` when available and otherwise an installed Python 3.12 plus pip. Existing environments are not deleted or recreated. An incomplete or incompatible environment produces an actionable error. Setup does not build, package, migrate, or upload data.

- `make test`: discover `tests/test_*.py` with the local interpreter.
- `make check`: run the tests, then the read-only packaged-database validator.
- `make run`: bind the existing FastHTML app to `127.0.0.1:8000`.

Without Make, `python3 scripts/dev.py <command>` provides the same behavior. Runtime serving still follows the existing database choice: explicit repository argument, then `ENDURANCEVIZ_DB_PATH`, then a local rebuild if present, then the packaged database. Explicit arguments take precedence so fixture tests cannot accidentally query a real database because of a process-wide environment setting.

## What CI proves

`.github/workflows/ci.yml` runs on pull requests and pushes to main/the P1 branch, using Ubuntu 24.04 and Python 3.12. Checkout and setup-python are pinned to verified immutable commit IDs. Permissions are `contents: read`; checkout does not persist its credential. Runs are bounded to 15 minutes and superseded runs on the same ref are canceled.

The workflow installs the existing pinned requirements, checks dependency compatibility, runs all tests, and executes `scripts/check_serving_2024.py`. It neither requests service credentials nor deploys the app. The integration-test application is explicitly pointed at the packaged artifact; synthetic repository fixtures retain their own databases.

The 19-check artifact validator reads projected columns directly from DuckDB:

1. Reuse all 16 existing P0 assertions: identity/account keys, dates, units, categories, coverage/weekly keys, missing-value behavior, reconciliation, normalized performances, and the released row-count baseline.
2. Require exactly 53 distinct Monday-starting rows per registered athlete, with December 30 as the one partial window.
3. Independently aggregate actual activities and reconcile P1 record counts and distinct UTC running days at each athlete/week key.
4. Independently verify weekly distance: missing/invalid recorded Run measurements block the whole-week distance; no Run records remains unavailable; otherwise sums agree within 0.001 metres.

The database opens read-only. P1 comparison views are temporary and connection-local. Output contains check names/status and aggregate failure counts, not individual athlete data. Tests intentionally corrupt calendars, counts, and missing-distance behavior to verify that the new checks can fail rather than only succeeding on the current artifact. The existing deployment test verifies the artifact's checksum and the `data/` exclusion rule.

## Limits

These checks do not establish complete training capture or approve legacy P0 coverage scores for P1 selection. They do not run the full raw-CSV rebuild, contact MongoDB/Strava/World Athletics, perform load testing, or validate the browser visually. The full rebuild remains an explicit separate workflow.

The current test suite still depends on the committed packaged database and one legacy activity CSV for integration/type-inventory tests. Synthetic unit fixtures already exist, but a small standalone non-sensitive demo dataset remains a separate unfinished P1 item. CI creates no new bulk data artifact.

## Verified results

On 2026-09-09, `make setup` and `make check` passed locally. Setup was also exercised in a separate fresh directory; the resulting environment independently passed all 79 tests and 19 artifact checks. The packaged database checksum remained unchanged.

The first [hosted Linux run](https://github.com/danielcwq/elite-endurance-viz/actions/runs/34398767117) passed at implementation commit `a5d6c6747b80386c4d350f9b6ca4950ceb106858`: dependency compatibility, 79 tests, 19 artifact checks, and patch whitespace validation. No production deployment was performed. These results verify the implemented checks, not completion of the broader P1 roadmap.
