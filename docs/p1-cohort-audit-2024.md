# P1 initial cohort audit

Database SHA-256: `047931ce47ea65940011137109b81b73248abd8f4bff33ca54d3c355ed8a970c`

Reproduce: `.venv/bin/python scripts/audit_cohorts_2024.py`

This is planning evidence, not a finalized analysis protocol. No training outcomes are compared here.

Registry: **3,609** unique athletes. P0 coverage-eligible: **585**. Eligible with an assigned primary event: **584**. Excluded by coverage: **3,024**. Coverage-eligible but missing an event: **1**.

Counts use one directory row per athlete and the existing P0 primary-event assignment. Eligible means the stored P0 high/moderate coverage flag; it does not establish representativeness or complete training capture. Recorded sex comes from the existing source classification.

Score bands below are provisional 50-point bins for inspecting sample sizes. They are not approved performance tiers. Scores use the maximum results score within the assigned primary discipline. The source performances have already been selected at 1,100 points or above; this dataset alone cannot estimate what was excluded below that threshold.

## Coverage inventory

| coverage | athletes | eligible |
| --- | --- | --- |
| high | 499 | 499 |
| low | 91 | 0 |
| moderate | 86 | 86 |
| unknown | 2933 | 0 |

## Primary event and recorded sex

| event | recorded sex | registry | eligible | high | moderate | excluded |
| --- | --- | --- | --- | --- | --- | --- |
| 10 Mile Road | female | 3 | 0 | 0 | 0 | 3 |
| 10 Mile Road | male | 12 | 1 | 1 | 0 | 11 |
| 10 km Road | female | 89 | 2 | 2 | 0 | 87 |
| 10 km Road | male | 61 | 1 | 0 | 1 | 60 |
| 10000m | female | 88 | 11 | 8 | 3 | 77 |
| 10000m | male | 185 | 26 | 22 | 4 | 159 |
| 15 km Road | female | 4 | 0 | 0 | 0 | 4 |
| 15 km Road | male | 3 | 0 | 0 | 0 | 3 |
| 1500m | female | 212 | 44 | 37 | 7 | 168 |
| 1500m | male | 314 | 109 | 99 | 10 | 205 |
| 20 km Road | female | 1 | 0 | 0 | 0 | 1 |
| 20 km Road | male | 4 | 0 | 0 | 0 | 4 |
| 3000m Steeplechase | female | 156 | 20 | 16 | 4 | 136 |
| 3000m Steeplechase | male | 167 | 45 | 37 | 8 | 122 |
| 5 km Road | female | 14 | 0 | 0 | 0 | 14 |
| 5 km Road | male | 5 | 0 | 0 | 0 | 5 |
| 5000m | female | 124 | 23 | 21 | 2 | 101 |
| 5000m | male | 173 | 49 | 42 | 7 | 124 |
| 800m | female | 243 | 32 | 27 | 5 | 211 |
| 800m | male | 288 | 76 | 67 | 9 | 212 |
| Half Marathon | female | 125 | 6 | 5 | 1 | 119 |
| Half Marathon | male | 222 | 22 | 20 | 2 | 200 |
| Marathon | female | 441 | 44 | 34 | 10 | 397 |
| Marathon | male | 674 | 73 | 60 | 13 | 601 |
| No primary event | male | 1 | 1 | 1 | 0 | 0 |

## Candidate score bands — planning only

| event | recorded sex | score band | eligible |
| --- | --- | --- | --- |
| 10 Mile Road | male | 1100–1149 | 1 |
| 10 km Road | female | 1100–1149 | 1 |
| 10 km Road | female | 1150–1199 | 1 |
| 10 km Road | male | 1200–1249 | 1 |
| 10000m | female | 1100–1149 | 6 |
| 10000m | female | 1150–1199 | 4 |
| 10000m | female | 1200–1249 | 1 |
| 10000m | male | 1100–1149 | 16 |
| 10000m | male | 1150–1199 | 9 |
| 10000m | male | 1200–1249 | 1 |
| 1500m | female | 1100–1149 | 23 |
| 1500m | female | 1150–1199 | 10 |
| 1500m | female | 1200–1249 | 9 |
| 1500m | female | 1250+ | 2 |
| 1500m | male | 1100–1149 | 59 |
| 1500m | male | 1150–1199 | 40 |
| 1500m | male | 1200–1249 | 7 |
| 1500m | male | 1250+ | 3 |
| 3000m Steeplechase | female | 1100–1149 | 12 |
| 3000m Steeplechase | female | 1150–1199 | 5 |
| 3000m Steeplechase | female | 1200–1249 | 3 |
| 3000m Steeplechase | male | 1100–1149 | 24 |
| 3000m Steeplechase | male | 1150–1199 | 20 |
| 3000m Steeplechase | male | 1200–1249 | 1 |
| 5000m | female | 1100–1149 | 13 |
| 5000m | female | 1150–1199 | 10 |
| 5000m | male | 1100–1149 | 16 |
| 5000m | male | 1150–1199 | 20 |
| 5000m | male | 1200–1249 | 10 |
| 5000m | male | 1250+ | 3 |
| 800m | female | 1100–1149 | 19 |
| 800m | female | 1150–1199 | 10 |
| 800m | female | 1200–1249 | 3 |
| 800m | male | 1100–1149 | 42 |
| 800m | male | 1150–1199 | 25 |
| 800m | male | 1200–1249 | 5 |
| 800m | male | 1250+ | 4 |
| Half Marathon | female | 1100–1149 | 4 |
| Half Marathon | female | 1150–1199 | 2 |
| Half Marathon | male | 1100–1149 | 15 |
| Half Marathon | male | 1150–1199 | 6 |
| Half Marathon | male | 1200–1249 | 1 |
| Marathon | female | 1100–1149 | 24 |
| Marathon | female | 1150–1199 | 19 |
| Marathon | female | 1200–1249 | 1 |
| Marathon | male | 1100–1149 | 36 |
| Marathon | male | 1150–1199 | 26 |
| Marathon | male | 1200–1249 | 10 |
| Marathon | male | 1250+ | 1 |
| No primary event | male | Missing score | 1 |
