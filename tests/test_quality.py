from __future__ import annotations

import unittest

import pandas as pd

from enduranceviz.quality import validate_frames


class QualityReportTests(unittest.TestCase):
    def test_duplicate_activity_and_regression_fail_validation(self) -> None:
        athletes = pd.DataFrame([{"athlete_id": "a"}])
        accounts = pd.DataFrame(
            [{"provider": "strava", "external_account_id": "10", "athlete_id": "a"}]
        )
        activity_row = {
            "activity_id": "duplicate",
            "athlete_id": "a",
            "provider": "strava",
            "external_account_id": "10",
            "start_at_utc": "2024-01-01T00:00:00Z",
            "distance_meters": 1000,
            "elapsed_seconds": 300,
            "moving_seconds": 300,
            "activity_category": "Run",
            "pace_seconds_per_kilometer": 300,
        }
        activities = pd.DataFrame([activity_row, activity_row])
        performances = pd.DataFrame(
            [{"athlete_id": "a", "discipline": "5000m", "mark_text": "14:00"}]
        )
        coverage = pd.DataFrame(
            [{"athlete_id": "a", "week_start_utc": "2024-01-01"}]
        )
        weekly = pd.DataFrame(
            [{
                "athlete_id": "a", "week_start_utc": "2024-01-01",
                "observation_status": "observed", "activity_count": 1,
                "run_distance_meters": 1000,
            }]
        )
        summaries = pd.DataFrame(
            [{
                "total_activity_count": 1, "total_run_distance_meters": 1000,
                "default_cohort_eligible": False,
            }]
        )
        contract = {
            "dataset": {"name": "enduranceviz-2024", "specification_version": "fixture"},
            "window": {
                "start_inclusive_utc": "2024-01-01T00:00:00Z",
                "end_exclusive_utc": "2025-01-01T00:00:00Z",
            },
            "quality_baseline": {
                "athlete_rows": 1, "external_account_rows": 1,
                "performance_rows": 1, "activity_rows": 1,
                "coverage_rows": 1, "weekly_rows": 1,
                "minimum_default_cohort_athletes": 0,
            },
        }
        report = validate_frames(
            athletes, accounts, performances, activities, coverage, weekly,
            summaries, contract,
        )
        failed = {check["name"] for check in report["checks"] if check["status"] == "fail"}
        self.assertIn("unique_activity_ids", failed)
        self.assertIn("row_count_and_coverage_regression", failed)
        self.assertEqual(report["status"], "fail")


if __name__ == "__main__":
    unittest.main()
