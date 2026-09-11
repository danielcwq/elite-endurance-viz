# P1 collection-evidence correction

Status: verified local draft on `danielc-p1-2024-analytics`, 2026-09-09. Not deployed, not an approved analytical inclusion protocol.

## What changed

The historical collector's generic exception handler writes `Week NN - No Data` for both empty results and collection failures (`Get_Data/strava_scrape_new.py`, lines 450–461). P0 treated the existence of these rows as observed collection and could produce zero weekly distance and rolling averages from them.

The opt-in P1 path in `enduranceviz/analytics.py` now:

- Marks selected legacy `No Data` evidence as unknown, with `AMBIGUOUS_LEGACY_NO_DATA`; it cannot satisfy the legacy complete-week flag.
- Preserves actual activities even when the weekly source says `No Data` or has another warning.
- Leaves metrics null when there are no extracted activities, including positive weekly summaries whose activity extraction is absent. Recorded counts remain available separately in the observability views; missing training is not asserted to be zero.
- Prevents those ambiguous weeks from generating four-week rolling metrics through the existing completeness gate.

This changes evidence interpretation, not athlete inclusion. No fraction-of-a-year threshold is needed or applied. P0 defaults remain explicit legacy behavior so the released dataset is reproducible. The serving application still uses the unchanged packaged P0 database.

## Verified effects across the entire registry

These are athlete-week counts across all 3,609 athletes, **not** counts limited to the 800m/1500m comparison.

| Check | Result |
| --- | ---: |
| Canonical activities preserved | 139,887 |
| Coverage and weekly rows, each, unchanged | 191,277 |
| Legacy `No Data` athlete-weeks marked unknown | 12,707 |
| Of those, weeks retaining actual activities | 286 |
| Previously complete weeks now unknown | 12,384 |
| Empty-week run distances changed from zero to null | 17,423 |
| Non-null four-week averages, before → draft | 22,003 → 9,231 |

The 17,423 includes other empty extracted weeks, not only `No Data` rows. The reduction in rolling averages is a consequence of removing unsupported zeros, not evidence that athletes trained less. Retained rolling formulas are diagnostic, not yet approved study measures.

## Reproduce and inspect

Run from the repository root with the project virtual environment:

```sh
.venv/bin/python scripts/build_p1_evidence_2024.py --output data/derived/2024/p1-evidence-v2-draft.1-verified
.venv/bin/python -m unittest discover -s tests -p 'test_*.py'
```

Choose a fresh output directory for another build; existing builds are refused, not overwritten. The verified output directory above already exists locally.

The builder reads the packaged database read-only and checks that the original source records reproduce every released P0 coverage field before applying the correction. Timestamp precision/timezone representations are normalized for that comparison. It verifies unchanged athlete/week keys, activity totals, and active-week run counts, distances, and durations. It checks that empty-week distances are null and ambiguous weeks are not complete.

Outputs are `data_coverage_2024.parquet`, `weekly_training_2024.parquet`, and `build.json`. The manifest records source and implementation hashes, code commit, dirty-worktree status, transitions, and limitations. It checks that inputs and implementation stayed unchanged during the build. The verified run was made before committing the changes, so its manifest truthfully records a dirty worktree and supplies exact implementation hashes rather than claiming the parent commit alone reproduces it.

Source database SHA-256: `047931ce47ea65940011137109b81b73248abd8f4bff33ca54d3c355ed8a970c`.

These generated artifacts live under the existing Git-ignored `data/derived/2024/` directory. The deployment `data/` exclusion remains intact. No new bulk data is committed or deployed.

Regression coverage includes ambiguous empty sequences, contradictory sources with actual runs, positive summaries without extracted activities, valid four-week sequences, refusal to overwrite previous builds, and content-sensitive provenance hashes. Full suite: 45 tests passing.

## Still required

- Investigate conflicting weekly snapshots without silently treating one as training truth.
- Inspect posting calendars across the approved event pair without a P0 eligibility filter.
- Define metric-specific handling for missing distance/duration. This draft preserves P0 active-week formulas, which can omit missing measurements; the `complete` label is only the legacy source-consistency flag, not complete measurement or training.
- Agree the study question, window, metrics, and comparison design with Daniel before implementing inclusion or final comparisons.
- Integrate corrected evidence into P1 analytical serving with schema/UI tests. Annual summaries, directory eligibility, and the deployed artifact have not been rebuilt by this script.
