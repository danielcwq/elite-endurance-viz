from __future__ import annotations

import unittest
from datetime import date

import pandas as pd

from enduranceviz.analytics import (
    build_coverage,
    build_weekly_training,
    reconciliation_report,
    select_weekly_evidence,
)


class CoverageAnalyticsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.athletes = pd.DataFrame([{"athlete_id": "athlete-1"}])
        self.accounts = pd.DataFrame(
            [{"athlete_id": "athlete-1", "external_account_id": "10"}]
        )

    def evidence_row(self, week: int, date_range: str) -> dict[str, object]:
        return {
            "external_account_id": "10",
            "week_start_utc": date(2024, 1, 1) + pd.Timedelta(weeks=week - 1),
            "Date Range": date_range,
            "Distance (km)": 0,
            "Time": "0m",
            "Elevation (m)": 0,
            "source_file": "fixture.csv",
            "source_row_number": week + 1,
            "source_priority": 1,
            "collection_completed_at_utc": pd.NaT,
        }

    def activity(self, week: int, activity_id: str, distance: float) -> dict[str, object]:
        week_start = date(2024, 1, 1) + pd.Timedelta(weeks=week - 1)
        return {
            "athlete_id": "athlete-1",
            "activity_id": activity_id,
            "week_start_utc": week_start,
            "start_at_utc": pd.Timestamp(week_start, tz="UTC") + pd.Timedelta(hours=8),
            "activity_category": "Run",
            "provider_activity_type": "Run",
            "distance_meters": distance,
            "moving_seconds": distance / 3,
            "elapsed_seconds": distance / 3 + 10,
        }

    def test_observed_zero_is_numeric_while_missing_is_null(self) -> None:
        evidence = pd.DataFrame(
            [
                self.evidence_row(1, "Week 1 - No Data"),
                self.evidence_row(2, "Activities for 8 Jan 2024 - 14 Jan 2024"),
            ]
        )
        activities = pd.DataFrame([self.activity(2, "a-1", 10_000)])
        coverage = build_coverage(
            self.athletes, self.accounts, activities, select_weekly_evidence(evidence)
        )
        weekly = build_weekly_training(coverage, activities, set())
        by_week = weekly.set_index("week_start_utc")
        self.assertEqual(by_week.loc[date(2024, 1, 1), "run_distance_meters"], 0)
        self.assertEqual(by_week.loc[date(2024, 1, 8), "run_distance_meters"], 10_000)
        self.assertTrue(pd.isna(by_week.loc[date(2024, 1, 15), "run_distance_meters"]))
        self.assertEqual(
            coverage.set_index("week_start_utc").loc[date(2024, 1, 15), "observation_status"],
            "missing",
        )

    def test_four_week_metric_requires_four_complete_weeks(self) -> None:
        ranges = [
            "Week 1 - No Data",
            "Week 2 - No Data",
            "Week 3 - No Data",
            "Week 4 - No Data",
        ]
        evidence = pd.DataFrame(
            [self.evidence_row(index, value) for index, value in enumerate(ranges, 1)]
        )
        activities = pd.DataFrame(
            columns=[
                "athlete_id", "activity_id", "week_start_utc", "start_at_utc",
                "activity_category", "provider_activity_type", "distance_meters",
                "moving_seconds", "elapsed_seconds",
            ]
        )
        coverage = build_coverage(
            self.athletes, self.accounts, activities, select_weekly_evidence(evidence)
        )
        weekly = build_weekly_training(coverage, activities, set())
        fourth = weekly.loc[weekly["week_start_utc"] == date(2024, 1, 22)].iloc[0]
        fifth = weekly.loc[weekly["week_start_utc"] == date(2024, 1, 29)].iloc[0]
        self.assertEqual(fourth["rolling_4w_observed_weeks"], 4)
        self.assertEqual(fourth["rolling_4w_run_distance_meters"], 0)
        self.assertTrue(pd.isna(fifth["rolling_4w_run_distance_meters"]))

    def test_reconciliation_detects_summary_or_weekly_drift(self) -> None:
        activities = pd.DataFrame([self.activity(1, "a-1", 10_000)])
        weekly = pd.DataFrame([{"run_distance_meters": 9_000, "activity_count": 1}])
        summaries = pd.DataFrame(
            [{"total_run_distance_meters": 10_000, "total_activity_count": 1}]
        )
        report = reconciliation_report(activities, weekly, summaries)
        self.assertEqual(report["run_distance_max_absolute_difference_meters"], 1_000)
        self.assertTrue(report["activity_count_reconciles"])

    def test_p1_no_data_is_unknown_and_does_not_create_rolling_zeros(self) -> None:
        evidence = pd.DataFrame([
            self.evidence_row(week, f"Week {week} - No Data") for week in range(1, 5)
        ])
        activities = pd.DataFrame([self.activity(5, 'actual-run', 5000)])
        coverage = build_coverage(
            self.athletes, self.accounts, activities, select_weekly_evidence(evidence),
            empty_policy='unknown',
        )
        first_four = coverage.iloc[:4]
        self.assertTrue(first_four['observation_status'].eq('unknown').all())
        self.assertFalse(first_four['is_complete_enough_week'].any())
        weekly = build_weekly_training(coverage, activities, set(), synthesize_observed_zeros=False)
        self.assertTrue(weekly.iloc[:4]['run_distance_meters'].isna().all())
        self.assertTrue(weekly.iloc[:4]['rolling_4w_run_distance_meters'].isna().all())
        self.assertEqual(weekly.iloc[4]['run_distance_meters'], 5000)

    def test_p1_conflicting_no_data_retains_real_activity(self) -> None:
        evidence = pd.DataFrame([self.evidence_row(1, 'Week 1 - No Data')])
        activities = pd.DataFrame([self.activity(1, 'actual-run', 5000)])
        coverage = build_coverage(
            self.athletes, self.accounts, activities, select_weekly_evidence(evidence),
            empty_policy='unknown',
        )
        self.assertEqual(coverage.iloc[0]['observation_status'], 'unknown')
        self.assertIn('SUMMARY_ACTIVITY_CONTRADICTION', coverage.iloc[0]['collection_error_code'])
        self.assertIn('AMBIGUOUS_LEGACY_NO_DATA', coverage.iloc[0]['collection_error_code'])
        weekly = build_weekly_training(coverage, activities, set(), synthesize_observed_zeros=False)
        self.assertEqual(weekly.iloc[0]['activity_count'], 1)
        self.assertEqual(weekly.iloc[0]['run_distance_meters'], 5000)

    def test_p1_summary_without_extracted_activities_does_not_invent_zero(self) -> None:
        evidence = pd.DataFrame([self.evidence_row(1, 'Activities for 1 Jan 2024 - 7 Jan 2024')])
        activities = pd.DataFrame([self.activity(2, 'actual-run', 5000)])
        coverage = build_coverage(
            self.athletes, self.accounts, activities, select_weekly_evidence(evidence),
            empty_policy='unknown',
        )
        weekly = build_weekly_training(coverage, activities, set(), synthesize_observed_zeros=False)
        self.assertFalse(coverage.iloc[0]['is_complete_enough_week'])
        self.assertTrue(pd.isna(weekly.iloc[0]['run_distance_meters']))

    def test_p1_four_positive_consistent_weeks_support_rolling_metric(self) -> None:
        ranges = ['Activities for 1 Jan 2024 - 7 Jan 2024', 'Activities for 8 Jan 2024 - 14 Jan 2024',
                  'Activities for 15 Jan 2024 - 21 Jan 2024', 'Activities for 22 Jan 2024 - 28 Jan 2024']
        evidence = pd.DataFrame([self.evidence_row(i, r) for i, r in enumerate(ranges, 1)])
        activities = pd.DataFrame([self.activity(i, str(i), 5000) for i in range(1, 5)])
        coverage = build_coverage(self.athletes, self.accounts, activities, select_weekly_evidence(evidence), empty_policy='unknown')
        weekly = build_weekly_training(coverage, activities, set(), synthesize_observed_zeros=False)
        self.assertEqual(weekly.iloc[3]['rolling_4w_run_distance_meters'], 20000)


if __name__ == "__main__":
    unittest.main()
