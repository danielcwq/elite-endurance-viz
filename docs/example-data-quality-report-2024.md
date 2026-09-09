# EnduranceViz 2024 data-quality report

- Dataset: `enduranceviz-2024`
- Specification: `1.0.0`
- Generated: `2026-09-09T01:43:40Z`
- Result: **PASS** (16 checks, 0 failures)

| Check | Result | Detail |
| --- | --- | --- |
| `unique_activity_ids` | PASS | 139887 unique IDs across 139887 rows |
| `unique_external_accounts` | PASS | 678 unique provider/account keys across 678 rows |
| `activity_athlete_foreign_keys` | PASS | Every activity athlete_id resolves to athletes |
| `activity_account_foreign_keys` | PASS | Every activity provider/account pair resolves to athlete_external_accounts |
| `performance_athlete_foreign_keys` | PASS | Every performance athlete_id resolves to athletes |
| `required_timestamps_parse` | PASS | 0 unparseable activity timestamps |
| `activity_snapshot_window` | PASS | All activity starts are inside the half-open 2024 UTC window |
| `nonnegative_activity_measurements` | PASS | {'distance_meters': 0, 'elapsed_seconds': 0, 'moving_seconds': 0} |
| `canonical_units_and_categories` | PASS | Canonical categories are valid, swim distance is suppressed, and pace has positive distance |
| `unique_coverage_keys` | PASS | 191277 unique coverage athlete-weeks |
| `unique_weekly_keys` | PASS | 191277 unique analytical athlete-weeks |
| `coverage_weekly_key_alignment` | PASS | Coverage and weekly tables have identical keys |
| `missing_is_not_zero` | PASS | Inactive missing/unknown weeks retain null analytical values |
| `activity_weekly_summary_reconciliation` | PASS | {'activity_run_distance_meters': 1184622339.8260002, 'weekly_run_distance_meters': 1184622339.8259997, 'summary_run_distance_meters': 1184622339.826, 'run_distance_max_absolute_difference_meters': 4.76837158203125e-07, 'activity_unique_count': 139887, 'weekly_activity_count': 139887, 'summary_activity_count': 139887, 'activity_count_reconciles': True} |
| `normalized_performance_representation` | PASS | Canonical performance rows contain one discipline and mark per row |
| `row_count_and_coverage_regression` | PASS | actual={'athlete_rows': 3609, 'external_account_rows': 678, 'performance_rows': 5305, 'activity_rows': 139887, 'coverage_rows': 191277, 'weekly_rows': 191277}, expected={'athlete_rows': 3609, 'external_account_rows': 678, 'performance_rows': 5305, 'activity_rows': 139887, 'coverage_rows': 191277, 'weekly_rows': 191277, 'minimum_default_cohort_athletes': 585}, default_cohort=585 |
