# Continuous performance points and recorded training: exploratory view

Score policy: `p1-primary-event-points-exploratory-v1`. Training policy: `p1-recorded-training-exploratory-v1.1`.

Source database SHA-256: `047931ce47ea65940011137109b81b73248abd8f4bff33ca54d3c355ed8a970c`.

## Question and definitions

Within each primary event and recorded-sex group, what does the relationship between stored 2024 World Athletics result points and recorded-week running summaries look like?

Daniel approved continuous points, separate sex groups, and the existing recorded-week summaries. The operational score definition below was disclosed before calculating pairs. This is an exploratory scatter view, not a fitted association model or a final study conclusion.

- One athlete contributes at most one point to each metric panel in their P0 primary event. Use the highest non-null `results_score` among stored 2024 performances in that event. Ties have the same score and do not create additional entries. Other-event results never replace a missing score.
- Keep athletes without a score or training metric in the inventory; omit only unavailable pairs from the relevant plot. There is no new point, annual-week, or P0 eligibility cutoff.
- Training values are each athlete’s median across full UTC weeks with recorded Runs. Distance uses only weeks where all Run distances are finite and nonnegative; Run-record counts use all recorded-running weeks. December 30–31 is excluded from these medians. Records are not independent sessions.
- Colour shows the number of weeks contributing to that particular metric, on a fixed 1–52 scale. It is not a confidence score. Actual activity records from source-warning weeks remain.

The snapshot’s pre-existing 1,100-point source floor restricts the available performance range. “Highest stored score” is not a claim to have every race in an athlete’s season. Primary-event assignment itself uses performance points. These limitations and selective public posting constrain interpretation; do not pool event/sex panels or treat training summaries as complete years. No p-values, fitted lines, correlation coefficients, or causal/predictive claims are calculated.

## Reproduce locally

```sh
.venv/bin/python scripts/analyze_performance_training_2024.py --figure-data data/derived/2024/p1-exploration/performance-training.json
.venv/bin/python scripts/plot_performance_training_2024.py data/derived/2024/p1-exploration/performance-training.json data/derived/2024/p1-exploration/performance-training
```

The plot command writes one distance figure and one Run-record-count figure. Install `requirements-analysis.txt` for plotting. Individual plot inputs and images stay local under Git-ignored `data/derived/`; this report contains aggregate counts only. Nothing is deployed.

## Score availability

| event | recorded sex | registry | scored athletes | missing score | primary event results | unscored results | score min | score max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 800m | female | 243 | 243 | 0 | 247 | 0 | 1100 | 1261 |
| 800m | male | 288 | 288 | 0 | 289 | 0 | 1100 | 1295 |
| 1500m | female | 212 | 212 | 0 | 212 | 0 | 1100 | 1295 |
| 1500m | male | 314 | 314 | 0 | 314 | 0 | 1100 | 1292 |
| 3000m Steeplechase | female | 156 | 156 | 0 | 158 | 0 | 1100 | 1285 |
| 3000m Steeplechase | male | 167 | 167 | 0 | 168 | 0 | 1100 | 1250 |
| 5000m | female | 124 | 124 | 0 | 131 | 0 | 1100 | 1238 |
| 5000m | male | 173 | 173 | 0 | 180 | 0 | 1101 | 1296 |
| 10000m | female | 88 | 88 | 0 | 89 | 0 | 1100 | 1309 |
| 10000m | male | 185 | 185 | 0 | 205 | 0 | 1100 | 1273 |
| Half Marathon | female | 125 | 125 | 0 | 125 | 0 | 1100 | 1270 |
| Half Marathon | male | 222 | 222 | 0 | 227 | 0 | 1101 | 1289 |

## Pair availability by metric

The four pair states partition each event/sex registry. Missing-score-only means training is available; missing-training-only means a score is available. No training value is filled with zero.

| event | recorded sex | metric | registry | paired | missing score only | missing training only | missing both |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 800m | female | distance_km | 243 | 22 | 0 | 221 | 0 |
| 800m | female | run_records | 243 | 22 | 0 | 221 | 0 |
| 800m | male | distance_km | 288 | 62 | 0 | 226 | 0 |
| 800m | male | run_records | 288 | 62 | 0 | 226 | 0 |
| 1500m | female | distance_km | 212 | 28 | 0 | 184 | 0 |
| 1500m | female | run_records | 212 | 28 | 0 | 184 | 0 |
| 1500m | male | distance_km | 314 | 91 | 0 | 223 | 0 |
| 1500m | male | run_records | 314 | 91 | 0 | 223 | 0 |
| 3000m Steeplechase | female | distance_km | 156 | 17 | 0 | 139 | 0 |
| 3000m Steeplechase | female | run_records | 156 | 17 | 0 | 139 | 0 |
| 3000m Steeplechase | male | distance_km | 167 | 37 | 0 | 130 | 0 |
| 3000m Steeplechase | male | run_records | 167 | 37 | 0 | 130 | 0 |
| 5000m | female | distance_km | 124 | 15 | 0 | 109 | 0 |
| 5000m | female | run_records | 124 | 15 | 0 | 109 | 0 |
| 5000m | male | distance_km | 173 | 39 | 0 | 134 | 0 |
| 5000m | male | run_records | 173 | 39 | 0 | 134 | 0 |
| 10000m | female | distance_km | 88 | 10 | 0 | 78 | 0 |
| 10000m | female | run_records | 88 | 10 | 0 | 78 | 0 |
| 10000m | male | distance_km | 185 | 19 | 0 | 166 | 0 |
| 10000m | male | run_records | 185 | 19 | 0 | 166 | 0 |
| Half Marathon | female | distance_km | 125 | 3 | 0 | 122 | 0 |
| Half Marathon | female | run_records | 125 | 3 | 0 | 122 | 0 |
| Half Marathon | male | distance_km | 222 | 18 | 0 | 204 | 0 |
| Half Marathon | male | run_records | 222 | 18 | 0 | 204 | 0 |

## Posting breadth among paired athletes

Week counts are metric-specific. Warning counts indicate retained source-warning running weeks, not that every plotted metric week has a warning. Inspect sparse and well-documented points together before deciding whether any numerical association summary is useful.

| event | recorded sex | metric | paired | paired score min | paired score max | weeks min | weeks median | weeks max | athletes with warning run weeks |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 800m | female | distance_km | 22 | 1100 | 1207 | 3 | 36.0 | 51 | 14 |
| 800m | female | run_records | 22 | 1100 | 1207 | 4 | 36.0 | 51 | 14 |
| 800m | male | distance_km | 62 | 1100 | 1292 | 1 | 42.0 | 51 | 43 |
| 800m | male | run_records | 62 | 1100 | 1292 | 1 | 43.0 | 51 | 43 |
| 1500m | female | distance_km | 28 | 1105 | 1266 | 2 | 29.5 | 52 | 14 |
| 1500m | female | run_records | 28 | 1105 | 1266 | 2 | 29.5 | 52 | 14 |
| 1500m | male | distance_km | 91 | 1100 | 1276 | 1 | 39.0 | 52 | 63 |
| 1500m | male | run_records | 91 | 1100 | 1276 | 1 | 40.0 | 52 | 63 |
| 3000m Steeplechase | female | distance_km | 17 | 1101 | 1226 | 3 | 40.0 | 51 | 12 |
| 3000m Steeplechase | female | run_records | 17 | 1101 | 1226 | 3 | 42.0 | 51 | 12 |
| 3000m Steeplechase | male | distance_km | 37 | 1114 | 1219 | 9 | 41.0 | 52 | 23 |
| 3000m Steeplechase | male | run_records | 37 | 1114 | 1219 | 9 | 41.0 | 52 | 23 |
| 5000m | female | distance_km | 15 | 1101 | 1199 | 10 | 44.0 | 51 | 8 |
| 5000m | female | run_records | 15 | 1101 | 1199 | 10 | 44.0 | 51 | 8 |
| 5000m | male | distance_km | 39 | 1103 | 1251 | 1 | 38.0 | 52 | 30 |
| 5000m | male | run_records | 39 | 1103 | 1251 | 1 | 38.0 | 52 | 30 |
| 10000m | female | distance_km | 10 | 1100 | 1215 | 3 | 41.0 | 52 | 7 |
| 10000m | female | run_records | 10 | 1100 | 1215 | 3 | 41.0 | 52 | 7 |
| 10000m | male | distance_km | 19 | 1102 | 1211 | 2 | 43.0 | 52 | 16 |
| 10000m | male | run_records | 19 | 1102 | 1211 | 2 | 43.0 | 52 | 16 |
| Half Marathon | female | distance_km | 3 | 1111 | 1170 | 17 | 20.0 | 34 | 1 |
| Half Marathon | female | run_records | 3 | 1111 | 1170 | 17 | 20.0 | 34 | 1 |
| Half Marathon | male | distance_km | 18 | 1101 | 1179 | 2 | 40.0 | 52 | 10 |
| Half Marathon | male | run_records | 18 | 1101 | 1179 | 2 | 40.0 | 52 | 10 |

Other/unknown recorded-sex registry athletes: 0. They remain in the tables but are not pooled into the approved women’s/men’s figures.

Next analytical checkpoint: inspect the scatter view before choosing any numerical association summary, coverage sensitivity analysis, or final inclusion rule. See [analysis decisions](p1-analysis-decisions.md).
