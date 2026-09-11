# P1 comparison review package

Release preparation authorized on 2026-09-11: shorten the heading to “See the
differences,” explain the counts and percentile range in plain language, update
the README, and open a PR with deployed-preview checks. “No plotted value” now
replaces the ambiguous table heading “Unavailable.” It counts registry athletes
without the selected chart value, not exclusions for low mileage. “Middle 50%”
includes metric units and is explicitly the 25th–75th percentile range of athlete
medians, not a confidence interval, accuracy estimate, or training prescription.
The calculation, identity-review warning, packaged database, and `data/`
deployment guardrail are unchanged. Production status is determined by the PR
and its deployment, not by the historical local-preview notes below.

Daniel's subsequent screenshot review on 2026-09-10 reopened the UI, overlapping
charts, event-assignment explanation, Marathon visibility, and proposed anomaly
examples. Full editorial case studies and unfinished items in the old P1 roadmap
are not requirements for this package. This is not a finished inferential study
or approval to deploy production.

## Open the preview

Run `make run`, then visit `http://127.0.0.1:8000/compare`.
The review session uses `http://127.0.0.1:8011/compare` on the P1 branch.

- Defaults: 800m versus 5000m, women and men as distinct overlaid series, distance
  distributions. Select women, men, or both without pooling groups.
- Select two of seven events (including Marathon), distance or Run-record count,
  and distributions or continuous performance points; press **Update**.
- GET parameters preserve the comparison. Selecting the same event twice displays
  it once. No score bands, minimum-week filter, fitted lines, or new metrics.
- Each dot is one athlete. Hover a dot or cohort key to highlight its series;
  click to pin the selection. Pinned details include a profile link and stored
  event/score portfolio. Reset or Escape clears selection. The values table
  provides keyboard/touch access to the same selection and profile links.
- One shared-axis chart replaces side-by-side panels. Distribution curves are
  empirical cumulative distributions (ECDFs): y is the share of the group at or
  below an x value. Ties share their full cumulative fraction. No smoothing or
  jitter. Men use dashed curves/hollow markers as well as distinct colours.
- Cohort keys report group medians and recording breadth; the expanded table
  reports the middle 50%, not a confidence interval. For even-sized groups the
  median averages the two central values; the ECDF's step at 50% need not sit
  exactly at that interpolated median.
- Performance view uses continuous stored points on x and recorded-week metric
  on y. Colours distinguish event/sex groups, not recording completeness.
- Registry, contributor, missing, contributing-week, source-warning and
  other/unknown-sex counts remain available alongside version/build/methods.
- `/recordings` lists all 460 athletes with stored activities, with name/event
  filters, recording-breadth/distance/name sorts, and 50-row pagination. No
  automatic quality or eligibility classification. A known, unconfirmed identity
  concern is shown on the affected comparison point and athlete profile.

## Static plots

Existing six-event figures remain unchanged and Git-ignored:

- `data/derived/2024/p1-exploration/training-distributions.png`
- `data/derived/2024/p1-exploration/performance-training-distance_km.png`
- `data/derived/2024/p1-exploration/performance-training-run_records.png`

They retain separate event/sex panels; static scatterplots colour by contributing
weeks. Reproduction commands are in [event exploration](p1-exploratory-training-2024.md)
and [performance exploration](p1-performance-training-2024.md). No generated image
or full athlete-level export is added to GitHub. `data/` stays excluded from deployment.

## Reconciliation and checks

The serving query reuses `ATHLETE_RECORDED_TRAINING_SQL` and
`PERFORMANCE_TRAINING_SQL` read-only. It caches full athlete summaries and filters
preview events afterwards. There is no new database, migration, Atlas write,
source-data change, or frontend framework dependency.

Default: 138 contributors (800m women 22 / men 62; 5000m women 15 / men 39).
Original six events: 361 metric/score pairs among 2,297 registry athletes.
Marathon adds 89 / 1,115, for 450 / 3,412 in the seven-event preview.
Tests reconcile every original-six-event athlete's values, denominators and
score with the static-analysis query. Missing values, recorded zero, missing
scores, duplicate event selection, invalid controls, empty charts, synthetic
identification, ECDF ties, sex selection and every event/metric/view combination
  are covered. Inventory tests cover all 460 entries, filters, unchanged event
assignment, read-only audit generation, and retention of the identity-review row.

Browser QA covers cohort isolation, pinned point details, responsive containment,
filter URLs, keyboard selection/reset, and preserved homepage search/map access.
The final local check passed 138 tests and 19 packaged-data validations. The
packaged database SHA-256 remains
`047931ce47ea65940011137109b81b73248abd8f4bff33ca54d3c355ed8a970c`.
Narrow charts/tables scroll within labelled keyboard regions. No screen-reader
or production load-test claim is made. The design skill guided Inter typography,
visual hierarchy, responsive layout and accessible controls while retaining
FastHTML/Pico. The interaction reference was the [DeepSWE blog](https://deepswe.datacurve.ai/blog/deepswe).

Production and the packaged P0 database are unchanged. This is a local review on
`danielc-p1-2024-analytics`, not a merge or deployment. See the
[recording review](p1-ui-and-recording-review.md) for the anomaly shortlist.
