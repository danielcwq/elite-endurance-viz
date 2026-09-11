# Run records are not necessarily training sessions

Database SHA-256: `047931ce47ea65940011137109b81b73248abd8f4bff33ca54d3c355ed8a970c`.

Reproduce: `.venv/bin/python scripts/audit_run_record_structure_2024.py`.

## Why this check was needed

The approved exploratory frequency measure counts stored Run records. Distinct records may be warm-ups, repetitions, recoveries, cool-downs, separate sessions, or overlapping recordings. Unique activity IDs alone do not establish independent training sessions.

Across the entire snapshot there are 0 repeated Run fingerprint groups using (athlete, exact start timestamp, distance, moving duration, elapsed duration). This checks exact record repetition, not near-duplicates or overlapping activities. No records were merged or removed.

## Three highest athlete median record counts

Selected deterministically across the six study events by median recorded-week Run count, then athlete ID. This is an outlier inspection, not an athlete exclusion rule. Record length and maximum daily counts below use all stored 2024 runs; weekly medians use full weeks.

| display name | event | recorded run weeks | median records per recorded week | median recorded running days | median record meters | median moving seconds | maximum daily records |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Ruken Tek | 3000m Steeplechase | 15 | 39.0 | 6.0 | 241.4 | 40.0 | 29 |
| Invida Mauriņa | 800m | 4 | 27.5 | 5.5 | 981.7 | 210.5 | 15 |
| Ignacio Erario | Half Marathon | 46 | 22.0 | 7.0 | 5498.9 | 1660.0 | 9 |

## Concrete trace: Ruken Tek, January 5

This date was inspected after the high-count outlier appeared. The ordered source records below are consistent with separately recorded repetitions; they do not prove a particular workout structure.

| day | run records | known distance km | first start | last start |
| --- | --- | --- | --- | --- |
| 2024-01-05 | 29 | 7.66 | 2024-01-05 07:36:47+00:00 | 2024-01-05 08:57:52+00:00 |

| activity id | start at utc | distance meters | moving seconds | elapsed seconds | source file | source row number |
| --- | --- | --- | --- | --- | --- | --- |
| 10625574255 | 2024-01-05 07:36:47+00:00 | 6002.84 | 1657.0 | 1661.0 | indiv_activities_full.csv | 116197 |
| 10625574354 | 2024-01-05 08:28:43+00:00 | 32.19 | 7.0 | 8.0 | indiv_activities_full.csv | 116196 |
| 10625574442 | 2024-01-05 08:29:42+00:00 | 32.19 | 7.0 | 7.0 | indiv_activities_full.csv | 116195 |
| 10625574698 | 2024-01-05 08:30:15+00:00 | 32.19 | 7.0 | 7.0 | indiv_activities_full.csv | 116194 |
| 10625574873 | 2024-01-05 08:30:44+00:00 | 32.19 | 7.0 | 7.0 | indiv_activities_full.csv | 116193 |
| 10625575030 | 2024-01-05 08:31:26+00:00 | 32.19 | 7.0 | 7.0 | indiv_activities_full.csv | 116192 |
| 10625575214 | 2024-01-05 08:31:58+00:00 | 32.19 | 7.0 | 7.0 | indiv_activities_full.csv | 116191 |
| 10625575276 | 2024-01-05 08:32:36+00:00 | 32.19 | 7.0 | 7.0 | indiv_activities_full.csv | 116190 |

## Recorded running days as a separate diagnostic

A recorded running day is a UTC date with at least one stored Run. It is unaffected by splitting one day into many records, but cannot distinguish single from double sessions or recover missing training. Within each athlete, take the median over weeks with recorded runs, then the median across athletes. This supports interpretation of the approved record-count measure; it does not replace it.

| event | recorded sex | athletes | median run records | median recorded running days |
| --- | --- | --- | --- | --- |
| 800m | female | 22 | 5.0 | 4.75 |
| 800m | male | 62 | 6.5 | 5.0 |
| 1500m | female | 28 | 3.0 | 3.0 |
| 1500m | male | 91 | 7.0 | 6.0 |
| 3000m Steeplechase | female | 17 | 7.0 | 6.0 |
| 3000m Steeplechase | male | 37 | 9.0 | 6.0 |
| 5000m | female | 15 | 8.0 | 6.0 |
| 5000m | male | 39 | 9.0 | 6.0 |
| 10000m | female | 10 | 5.0 | 4.5 |
| 10000m | male | 19 | 9.0 | 6.0 |
| Half Marathon | female | 3 | 9.0 | 6.0 |
| Half Marathon | male | 18 | 4.5 | 3.75 |

## Decision boundary

- Keep the original metric labeled Run record count, not training-session count. The approved values are unchanged.
- Recorded running days can accompany record counts without inventing a session boundary.
- Defining sessions requires an explicit protocol for elapsed-time overlaps, gaps, missing duration, and separately recorded warm-up/repetition/cool-down activity. No gap threshold or session grouping has been selected.
- The inspected pattern suggests record fragmentation. It does not justify excluding an athlete or dropping short runs.
- Distance sums still need overlap/near-duplicate checks; an exact-fingerprint check is not sufficient validation of every sum.

Validation: recorded run-day counts are bounded by calendar days and Run records, and their total reconciles to independent distinct athlete/UTC-date counts. Production is unchanged.
