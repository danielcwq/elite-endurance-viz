# Weekly source conflicts and posting calendars

Database SHA-256: `047931ce47ea65940011137109b81b73248abd8f4bff33ca54d3c355ed8a970c`

Reproduce: `.venv/bin/python scripts/audit_source_conflicts_2024.py`

Scope: all athletes assigned to 800m/1500m. No P0 coverage filter or annual-week cutoff. Sources are unchanged; classifications do not clear warnings or choose a different source.

## Source disagreements

Original source rows reproduce the exact 611 full athlete-week conflict keys in the packaged database. The classification table counts 611 account-weeks; an athlete can own multiple accounts.

| Diagnostic category | Account-weeks |
| --- | ---: |
| different_date_range_text | 9 |
| different_totals_or_missing_fields | 117 |
| no_data_vs_summary | 485 |

In 185 conflicting account-weeks, the existing selection rule chooses a `No Data` row.

Categories are exclusive, in order: No Data versus another summary; differing date-range text; different totals or missing fields; otherwise equivalent numeric/time formatting. Numeric comparison uses exact decimal equality, not an arbitrary tolerance. A differing total does not by itself identify the correct source.

As a secondary diagnostic, 54 of the differing-total account-weeks agree after rounding distance to 0.1 km and elevation to 1 m, with equal parsed time. These precisions match the display-style values in the examples; they are not approved error tolerances and do not clear warnings. The old collector converts displayed miles by 1.60934 and feet by 0.3048 (`Get_Data/strava_scrape_new.py:352–383`), so unit conversion can produce extra decimal places. Compatibility with rounding is not proof that two captures are identical.

## Traceable examples

One example per category, selected deterministically by account/week order. These illustrate source defects, not representative athlete case studies. The existing source preference is not proof of correctness.

### different_date_range_text: Corentin Le Clezio, week starting 2024-11-04

Existing selected source: `data/tempdata/metadata_EtienneDAGUINOS_to_JamesCORRIGAN_w45-52_20250207_142225.csv`.

| File and CSV row | Date range | Distance (km) | Time | Elevation (m) |
| --- | --- | ---: | --- | ---: |
| data/raw_data/batch_45_40rows.csv:631 | Activities for 4 Nov 2024 - 10 Nov 2024 | 126.011322 | 8h 20m | 335.88960000000003 |
| data/tempdata/metadata_EtienneDAGUINOS_to_JamesCORRIGAN_w45-52_20250207_142225.csv:18 | Activities for 4 Nov 2024 - 10 Nov 2024 | 126.0 | 8h 20m | 336.0 |
| data/tempdata/metadata_EtienneDAGUINOS_to_JamesCORRIGAN_w45-52_20250207_142225.csv:274 | Activities for 3 Feb 2025 - 9 Feb 2025 | 49.1 | 2h 40m | 250.0 |

### different_totals_or_missing_fields: Davis Bove, week starting 2024-11-04

Existing selected source: `data/tempdata/metadata_DanielARCE_to_StanNIESTEN_w45-52_20250206_175102.csv`.

| File and CSV row | Date range | Distance (km) | Time | Elevation (m) |
| --- | --- | ---: | --- | ---: |
| data/raw_data/batch_23_meta_20.csv:541 | Activities for 4 Nov 2024 - 10 Nov 2024 | 153.04823399999998 | 9h 24m | 1217.9808 |
| data/tempdata/metadata_DanielARCE_to_StanNIESTEN_w45-52_20250206_175102.csv:82 | Activities for 4 Nov 2024 - 10 Nov 2024 | 153.0 | 9h 24m | 1218.0 |

### no_data_vs_summary: Allie Wilson, week starting 2024-12-02

Existing selected source: `data/tempdata/metadata_JohnKORIR_to_ViolaCHEPNGENO_w45-52_20250211_153308.csv`.

| File and CSV row | Date range | Distance (km) | Time | Elevation (m) |
| --- | --- | ---: | --- | ---: |
| data/raw_data/batch_54_meta_40.csv:1550 | Week 49 - No Data | 0.0 | 0h 0m | 0.0 |
| data/tempdata/metadata_JohnKORIR_to_ViolaCHEPNGENO_w45-52_20250211_153308.csv:30 | Activities for 2 Dec 2024 - 8 Dec 2024 | 6.1 | 1h 0m | 167.0 |

## Posting calendars

The next table counts athletes with at least one stored Run in each UTC calendar month. Months are display bins, not inclusion requirements. This uses activity timestamps, not weekly summaries, and includes December 30–31. It cannot establish intentional race-only or sporadic posting.

| month | event | athletes with recorded runs |
| --- | --- | --- |
| 2024-01 | 1500m | 87 |
| 2024-01 | 800m | 72 |
| 2024-02 | 1500m | 84 |
| 2024-02 | 800m | 72 |
| 2024-03 | 1500m | 86 |
| 2024-03 | 800m | 74 |
| 2024-04 | 1500m | 86 |
| 2024-04 | 800m | 71 |
| 2024-05 | 1500m | 90 |
| 2024-05 | 800m | 70 |
| 2024-06 | 1500m | 86 |
| 2024-06 | 800m | 71 |
| 2024-07 | 1500m | 89 |
| 2024-07 | 800m | 68 |
| 2024-08 | 1500m | 92 |
| 2024-08 | 800m | 71 |
| 2024-09 | 1500m | 87 |
| 2024-09 | 800m | 68 |
| 2024-10 | 1500m | 96 |
| 2024-10 | 800m | 68 |
| 2024-11 | 1500m | 97 |
| 2024-11 | 800m | 72 |
| 2024-12 | 1500m | 92 |
| 2024-12 | 800m | 68 |

## Within-year gaps among accounts with recorded runs

Full-week diagnostics below exclude only the two-day final week. Counts are distributions among athletes with any recorded 2024 run, not proposed eligibility cutoffs. Gaps include the beginning and end of the year.

| event | athletes | minimum run weeks | median run weeks | maximum run weeks | median longest gap weeks |
| --- | --- | --- | --- | --- | --- |
| 1500m | 119 | 1 | 39.0 | 52 | 5.0 |
| 800m | 84 | 1 | 41.0 | 51 | 5.0 |


## Consequences for P1

- Do not discard actual activity records because a weekly CSV contradicts them.
- Do not clear source warnings merely because a later or lexically preferred file exists.
- Posting calendars describe stored observations. They cannot label an athlete race-only without additional evidence.
- Choose the comparison question before deciding whether any minimum observation window is relevant.
