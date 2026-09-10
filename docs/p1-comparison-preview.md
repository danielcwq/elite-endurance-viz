# P1 comparison review package

Daniel narrowed the scope on 2026-09-10: comparison preview plus static plots,
then stop development for review. Athlete case studies are removed. Unfinished
items in the old P1 roadmap are deferred, not requirements for this package.
This is not a finished inferential study or an approval to deploy production.

## Open the preview

Run `make run`, then visit `http://127.0.0.1:8000/compare`.
The review session uses `http://127.0.0.1:8011/compare` on the P1 branch.

- Defaults: 800m versus 5000m, separate women/men, distance distributions.
- Select two of the six approved events, distance or Run-record count, and
  distributions or continuous performance points; press **Update**.
- GET parameters preserve the comparison in the URL. Selecting the same event
  twice displays that event once. No score bands, minimum-week filter, ranks,
  fitted lines, or new metrics are introduced.
- Each dot is one athlete. Hover for exact values and recording breadth; click
  for the existing athlete profile. Expand the values table for keyboard/touch
  access to the same values, week counts, warnings, scores, and profile links.
- Shared axes across the displayed panels; distribution jitter is display-only.
  Orange diamond is the median; line is the interquartile range of athlete
  medians, not a confidence interval. Static scatterplots additionally encode
  contributing weeks by colour; the preview uses counts and per-athlete details.
- Registry and metric/paired contributor counts, missing counts, median
  contributing weeks, source warnings, dataset version, build date, and methods
  are visible. Other/unknown recorded sex is inventoried without pooling.

## Static plots

Generated files stay local and Git-ignored:

- `data/derived/2024/p1-exploration/training-distributions.png`
- `data/derived/2024/p1-exploration/performance-training-distance_km.png`
- `data/derived/2024/p1-exploration/performance-training-run_records.png`

Reproduction commands are in [event exploration](p1-exploratory-training-2024.md)
and [performance exploration](p1-performance-training-2024.md). All three were
regenerated and visually inspected for this review package. No generated image
or athlete-level export is added to GitHub, and `data/` remains excluded from deployment.

## Reconciliation and checks

The serving query reuses `ATHLETE_RECORDED_TRAINING_SQL` and
`PERFORMANCE_TRAINING_SQL`, reading the database without modification. It caches
only the selected-event athlete summaries; there is no new database, migration,
Atlas write, source-data change, or frontend framework dependency.

The default view has 138 contributors: 800m women 22 / men 62, and 5000m women
15 / men 39. Across all six events, 361 of 2,297 registry athletes have a metric
and primary-event score. Tests compare every athlete's values, denominators, and
score directly with the reproducible static-analysis query. Missing values,
recorded zero, absent scores, duplicate event selections, invalid controls,
empty panels, synthetic identification, and all event/metric/view combinations
are covered.

Browser checks on 2026-09-10: desktop distribution and points layouts inspected;
filter submission produced the expected stable URL; 390px and 768px viewports
contained page content. Narrow charts/tables scroll within labelled keyboard
regions. Enter opens the values table; ArrowRight scrolls it. Mobile controls
rendered at 16px with 50px heights. No screen-reader or production load-test
claim is made. The design skill guided responsive grids and accessible controls,
while preserving the existing FastHTML/Pico conventions.

Production and the packaged P0 database are unchanged. This package is for local
review on `danielc-p1-2024-analytics`, not a merge or deployment.
