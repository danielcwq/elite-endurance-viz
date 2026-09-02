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


if __name__ == "__main__":
    unittest.main()
