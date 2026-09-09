# P1 activity-table filters

Implemented on `danielc-p1-2024-analytics`; not merged or deployed to production.

## Behavior

The existing athlete page now has a GET form immediately above its activity table. Dates include both endpoints in UTC and stay within the 2024 snapshot. Empty date fields mean the corresponding snapshot boundary. Category choices are All, Run, Ride, Swim, and Other; the table preserves each record's original provider type.

These controls filter **only the activity table**. The full-year running charts, medians, warning counts, and season-best cards do not change. This boundary is stated directly above the form. Chart-window controls remain unfinished P1 work; no partial-week metric or new analytical denominator is introduced here.

The URL stores `start`, `end`, `category`, and (after page one) `page`. Previous/next links retain the filters. Applying filters starts at page one. Reset returns to all 2024 activities. “Link to these filters” provides a stable first-page link; the address bar also represents the current page. Both target the activity section with `#activities`.

Matched counts accompany the table. Out-of-range page numbers resolve to the last available page, including page one for an empty result. Invalid dates, reversed/out-of-snapshot ranges, invalid categories, and invalid page numbers return HTTP 400 with a reason and the unfiltered profile path. An empty valid result has no table or pagination and does not claim zero training.

## Implementation and verification

- A validated immutable filter object converts inclusive dates into timezone-aware half-open UTC bounds. Repository queries use bound parameters and the same predicates for the count and rows.
- Counts and pagination remain scoped to one athlete, ordered by timestamp and activity ID. Page size is still capped at 50; the UI uses 30. No bulk export or unbounded result endpoint was added.
- Six new tests cover leap day, both midnight boundaries, timezone-offset input timestamps, all categories, original-type preservation, athlete isolation, stable pagination, large page numbers, injection-like input, invalid/empty states, unchanged charts, and direct packaged-database reconciliation.
- Full suite: 72 tests passed.
- Local browser checks cover desktop and 390-pixel mobile layout, form submission, keyboard submission, reset, empty results, and URL persistence. Controls stack on mobile; the table scrolls within its own region rather than widening the page.
- No database artifact, runtime dependency, production configuration, or `data/` deployment guardrail changed.

Example local URL:

```text
http://127.0.0.1:8011/athlete/29430aed-6031-400f-82c0-c5ec7f7a3e7a?start=2024-01-01&end=2024-03-31&category=Run#activities
```
