# P1 athlete timeline preview

Status: implemented and locally verified on `danielc-p1-2024-analytics`. No production merge or deployment was performed for this change.

## What changed

The existing `/athlete/{athlete_id}` page now shows the approved recorded-week distance and Run-record medians, plus the separately documented recorded-running-day diagnostic. Three server-rendered SVG timelines show all 53 calendar-week rows. The final two-day week has a dashed outline and is excluded from the full-week medians.

Gaps stay unavailable. A recorded zero distance is a distinct zero marker. Source-warning records remain visible with an amber outline and a warning count. An expandable HTML table gives exact weekly values and evidence labels. The adjacent metric definitions specify the window, denominators, recording-granularity caveat, and calculation policy.

The old P0 summary card set (including calendar-week averages and weighted pace) and legacy coverage-score headline are no longer shown on this P1 profile. They were not silently reused as approved P1 measures. Strava links, canonical season-best cards, the activity table, and pagination remain. Other P0 homepage/map surfaces have not been redesigned by this change.

The timestamp formatter also now converts timezone-aware values to UTC before labeling them UTC. The earlier formatter could display a connection-local time with the wrong UTC label.

## Data path and deployment boundary

`ServingRepository.recorded_weeks()` scopes the canonical activity and coverage tables to one athlete before running the shared exploratory SQL. It returns at most the snapshot's 53 weekly rows and uses a bounded 128-profile cache. The rendering layer uses standard-library medians; no pandas, matplotlib, extra chart service, or new runtime dependency is required by the web app.

The read-only database is unchanged. No additional bulk data is committed, and the existing `data/` deployment exclusion remains intact. The SQL derives distance from actual records with complete recorded measurements, not the old P0 synthetic weekly zeros. Source-warning classification remains diagnostic, not an eligibility filter or proof of complete capture.

## Verification

- Full suite: 66 tests passing, including seven new profile tests.
- Scoped profile results match the shared exploratory SQL for Ruken Tek and Jack Balick.
- Missing/zero/partial-week distinctions, source-warning rendering, escaped source-error text, and UTC conversion have regression coverage.
- Real HTTP profile response has one H1, three accessible SVG charts, and all 53 weekly table rows.
- Browser checks: normal desktop and 390-pixel mobile widths; no horizontal page overflow. Charts and tables scroll inside their own regions. Keyboard Enter opens the weekly-data disclosure.
- Browser examples: Ruken Tek (record fragmentation and long gap), Jack Balick (42 warning-bearing running weeks), Abdurrahman Gediklioğlu (missing distance and source warning), and Samuel Austin (no recorded runs).
- The existing light/dark CSS pattern is preserved. Visual verification in this pass covered light mode; a dark-mode visual pass remains part of P1 product QA.

## Run locally

```sh
.venv/bin/python -m uvicorn main:app --host 127.0.0.1 --port 8011 --reload
```

Example path: `/athlete/29430aed-6031-400f-82c0-c5ec7f7a3e7a#recorded-training`.

Still pending: rolling-volume protocol/UI, other fingerprint metrics, filters and filtered URLs, cohort comparison UI, and remaining P1 study and product QA. No session grouping or new analytical eligibility threshold was introduced.
