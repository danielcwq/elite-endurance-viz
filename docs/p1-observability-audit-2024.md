# Collection and posting audit: 800m versus 1500m

Database SHA-256: `047931ce47ea65940011137109b81b73248abd8f4bff33ca54d3c355ed8a970c`

Reproduce: `.venv/bin/python scripts/audit_observability_2024.py`

Scope: all athletes whose P0 primary event is 800m or 1500m, including athletes excluded by the old coverage score. No annual-week cutoff or new inclusion rule is applied.

Weekly diagnostics use the 52 full Monday–Sunday weeks in 2024. Annual activity counts also include December 30–31; that partial week is excluded only from the full-week diagnostics.

## Definitions

| Evidence state | What it establishes |
| --- | --- |
| activity_and_weekly_record | Activity records and a weekly record exist with no P0 source warning. This is not proof of complete capture. |
| ambiguous_empty_record | Legacy weekly record exists but no activities; the old collector could emit this after an exception. Empty capture is unconfirmed. |
| source_warning | P0 detected a disagreement or other source warning. Activity records, where present, are retained for inspection. |
| activity_without_weekly_record | Actual activities exist without corresponding weekly collection evidence. |
| no_collection_evidence | Neither activities nor an observed weekly record exist. |

“Recorded runs” and “weeks with runs” count records in the snapshot. Zero records does not establish zero training or deliberate non-posting. Longest gaps include gaps at both ends of the year. Known distance sums omit unavailable measurements and are not asserted to be complete totals.

## Athlete inventory

| event | recorded sex | registry | legacy eligible | any recorded runs | no recorded activities |
| --- | --- | --- | --- | --- | --- |
| 1500m | female | 212 | 44 | 28 | 184 |
| 1500m | male | 314 | 109 | 91 | 223 |
| 800m | female | 243 | 32 | 22 | 221 |
| 800m | male | 288 | 76 | 62 | 225 |

## Full athlete-week evidence

| evidence state | athlete weeks | with activities | legacy eligible athlete weeks |
| --- | --- | --- | --- |
| activity_and_weekly_record | 6469 | 6469 | 6457 |
| activity_without_weekly_record | 99 | 99 | 92 |
| ambiguous_empty_record | 5783 | 0 | 5626 |
| no_collection_evidence | 40019 | 0 | 465 |
| source_warning | 2594 | 608 | 932 |

## Posting counts — diagnostic bins, not eligibility categories

These bins describe the shape of the stored data. They do not classify athletes as race-only, complete posters, or suitable for an analysis. No training-volume comparison is calculated.

| event | annual recorded runs | athletes | legacy eligible |
| --- | --- | --- | --- |
| 1500m | 0 | 407 | 37 |
| 1500m | 101+ | 76 | 73 |
| 1500m | 11–100 | 31 | 31 |
| 1500m | 1–10 | 12 | 12 |
| 800m | 0 | 447 | 25 |
| 800m | 101+ | 62 | 61 |
| 800m | 11–100 | 19 | 19 |
| 800m | 1–10 | 3 | 3 |

## Source warnings

| collection error code | athlete weeks | with activities |
| --- | --- | --- |
| SUMMARY_ACTIVITY_CONTRADICTION | 1958 | 3 |
| CONFLICTING_WEEKLY_SOURCE_ROWS | 422 | 411 |
| CONFLICTING_WEEKLY_SOURCE_ROWS\|SUMMARY_ACTIVITY_CONTRADICTION | 182 | 174 |
| WEEK_RANGE_MISMATCH | 14 | 14 |
| SUMMARY_ACTIVITY_CONTRADICTION\|WEEK_RANGE_MISMATCH | 11 | 0 |
| CONFLICTING_WEEKLY_SOURCE_ROWS\|WEEK_RANGE_MISMATCH | 6 | 6 |
| CONFLICTING_WEEKLY_SOURCE_ROWS\|SUMMARY_ACTIVITY_CONTRADICTION\|WEEK_RANGE_MISMATCH | 1 | 0 |

## Two traced examples

These are the examples already discussed with Daniel, not representative case studies. Names identify source records; no posting intention is inferred.

| display name | week start utc | legacy coverage status | evidence state | posted activities | posted runs | evidence source |
| --- | --- | --- | --- | --- | --- | --- |
| Abdurrahman Gediklioğlu | 2024-01-15 | complete | ambiguous_empty_record | 0 | 0 | data/raw_data/batch_17_meta.csv |
| Jack Balick | 2024-01-01 | observed_with_warning | source_warning | 7 | 7 | data/raw_data/batch_27_30r.csv |

## Interpretation and next work

- The 26/39-week proposal is withdrawn. Annual coverage is not a prerequisite for exploration.
- A legacy high/moderate score is not an approved P1 inclusion filter.
- Reconcile source conflicts and inspect activity calendars before choosing analysis windows.
- Activity-only weeks remain evidence of posted activity, even when weekly summaries are absent.
- A successful collection record cannot establish completeness of actual athlete training.
- This diagnostic layer does not rebuild the P0 artifact or change deployed profile metrics. The P0 ambiguous-zero behaviour remains a known limitation until a separately validated pipeline correction.
