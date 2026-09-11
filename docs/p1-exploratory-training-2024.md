# Exploratory recorded-week training comparisons

Calculation policy: `p1-recorded-training-exploratory-v1.1`. Source database SHA-256: `047931ce47ea65940011137109b81b73248abd8f4bff33ca54d3c355ed8a970c`.

Reproduce: `.venv/bin/python scripts/analyze_event_training_2024.py`.

For the local figure, install `requirements-analysis.txt`, then run:

```sh
.venv/bin/python scripts/analyze_event_training_2024.py --figure-data data/derived/2024/p1-exploration/training-figure.json
.venv/bin/python scripts/plot_event_training_2024.py data/derived/2024/p1-exploration/training-figure.json data/derived/2024/p1-exploration/training-distributions.png
```

Figure inputs and PNG are local, Git-ignored outputs; no new athlete-level data is published.

## Question and interpretation

How do publicly observed running distance and run frequency vary across event specializations? 800m versus 5000m leads; 1500m provides context, 10000m extends the track comparison, steeplechase remains separate, and half marathon is an exploratory extension.

These tables describe **weeks with recorded runs**, not all training weeks or a complete year. Each contributing athlete supplies one median. Event/recorded-sex cells then summarize those athlete medians. This exploratory view was approved by Daniel in [P1 analysis decisions](p1-analysis-decisions.md); it is not a finalized inclusion protocol or a confirmatory test.

No annual-week or P0 coverage cutoff is applied. Full weeks are Monday–Sunday UTC; December 30–31 is retained in annual record counts only. Source-warning weeks retain their actual activities. Weekly distance is unavailable when any recorded run lacks a finite, nonnegative distance. Run counts remain usable as record counts. An athlete with no recorded runs remains in the inventory but contributes no running-week summary.

Run records must not be interpreted as independent training sessions: the [record-structure audit](p1-run-record-structure-2024.md) traces separately recorded short efforts and provides recorded running days as a complementary diagnostic. No session grouping has been applied.

## Contributors and measurement availability

| event | recorded sex | registry | any run 2024 | frequency contributors | distance contributors | recorded run weeks | distance unavailable weeks | source warning run weeks |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 800m | female | 243 | 22 | 22 | 22 | 726 | 1 | 53 |
| 800m | male | 288 | 62 | 62 | 62 | 2267 | 15 | 199 |
| 1500m | female | 212 | 28 | 28 | 28 | 823 | 0 | 98 |
| 1500m | male | 314 | 91 | 91 | 91 | 2978 | 9 | 248 |
| 3000m Steeplechase | female | 156 | 17 | 17 | 17 | 615 | 5 | 127 |
| 3000m Steeplechase | male | 167 | 37 | 37 | 37 | 1341 | 4 | 30 |
| 5000m | female | 124 | 15 | 15 | 15 | 576 | 0 | 8 |
| 5000m | male | 173 | 39 | 39 | 39 | 1249 | 1 | 72 |
| 10000m | female | 88 | 10 | 10 | 10 | 375 | 1 | 33 |
| 10000m | male | 185 | 19 | 19 | 19 | 754 | 2 | 46 |
| Half Marathon | female | 125 | 3 | 3 | 3 | 71 | 0 | 13 |
| Half Marathon | male | 222 | 18 | 18 | 18 | 596 | 1 | 48 |

## Distribution of athlete recorded-week medians

Q1 and Q3 are continuous 25th/75th percentiles across athlete medians, **not confidence intervals**. Distance and frequency summarize their own contributing weeks. Small cells are shown descriptively, without significance tests or generalization to the event population.

| event | recorded sex | distance q1 km | distance median km | distance q3 km | frequency q1 | frequency median | frequency q3 | median contributing run weeks |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 800m | female | 11.9 | 35.3 | 48.9 | 2.0 | 5.0 | 9.75 | 36.0 |
| 800m | male | 30.1 | 49.9 | 62.9 | 4.0 | 6.5 | 9.88 | 43.0 |
| 1500m | female | 12.8 | 30.0 | 71.5 | 1.0 | 3.0 | 7.13 | 29.5 |
| 1500m | male | 35.2 | 73.6 | 97.4 | 3.0 | 7.0 | 10.75 | 40.0 |
| 3000m Steeplechase | female | 51.6 | 73.5 | 90.3 | 5.0 | 7.0 | 10.0 | 42.0 |
| 3000m Steeplechase | male | 59.8 | 89.8 | 111.8 | 5.0 | 9.0 | 11.0 | 41.0 |
| 5000m | female | 81.6 | 94.5 | 100.4 | 7.0 | 8.0 | 11.5 | 44.0 |
| 5000m | male | 33.1 | 104.0 | 122.5 | 2.5 | 9.0 | 12.0 | 38.0 |
| 10000m | female | 45.1 | 57.7 | 84.8 | 4.0 | 5.0 | 8.75 | 41.0 |
| 10000m | male | 54.1 | 85.7 | 127.0 | 5.0 | 9.0 | 11.0 | 43.0 |
| Half Marathon | female | 54.0 | 77.9 | 85.7 | 5.0 | 9.0 | 10.0 | 20.0 |
| Half Marathon | male | 26.2 | 50.0 | 109.0 | 1.0 | 4.5 | 10.0 | 40.0 |

## What remains unresolved

- Differences can reflect posting/capture patterns, differing observed portions of the year, measurement availability, and selection into the source registry. They are not estimates of differences in complete training.
- Source-warning counts are exposed, not used as a silent exclusion criterion. No warning does not prove complete capture.
- Final comparison window, inclusion, uncertainty method, and performance tiers require review. Do not promote these exploratory tables to homepage findings.
- Session pace, intensity, and steeple-specific technical work are not inferred from these two measures.

Checks: one summary per directory athlete, all canonical Run records reconcile to summaries, and unavailable weekly distances remain null. The production artifact is opened read-only and unchanged.
