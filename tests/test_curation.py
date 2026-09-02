from __future__ import annotations

import unittest

import pandas as pd

from enduranceviz.curation import (
    canonicalize_activities,
    canonicalize_performances,
    parse_performance_mark,
)


VALIDATION = {
    "duplicate_distance_absolute_tolerance_meters": 20,
    "duplicate_distance_relative_tolerance": 0.005,
    "moving_elapsed_absolute_tolerance_seconds": 5,
    "moving_elapsed_relative_tolerance": 0.01,
    "maximum_elapsed_seconds": 604800,
    "maximum_run_distance_meters": 500000,
    "maximum_ride_distance_meters": 1000000,
}


def activity_row(**overrides):
    row = {
        "Serial": 1,
        "Athlete ID": 123,
        "Athlete Name": "Runner One",
        "Activity ID": 1000,
        "Activity Name": "Morning Run",
        "Description": None,
        "Start Date": "2024-01-03 10:00:00+00:00",
        "Elapsed Time": 1800,
        "Type": "Run",
        "Location": None,
        "Pace (min/mi)": 6.0,
        "Pace (min/km)": 3.7,
        "Time (min)": 29.0,
        "Distance (km)": 8.0,
        "Activity Time (s)": 1740,
        "Time": None,
    }
    row.update(overrides)
    return row


class ActivityCurationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.accounts = pd.DataFrame(
            [{"external_account_id": "123", "athlete_id": "athlete-1"}]
        )

    def test_equivalent_duplicate_is_deduplicated_and_audited(self) -> None:
        raw = pd.DataFrame(
            [
                activity_row(**{"Distance (km)": 8.007, "Serial": 1}),
                activity_row(**{"Distance (km)": 8.0, "Serial": 2}),
            ]
        )
        curated, quarantine, report = canonicalize_activities(raw, self.accounts, VALIDATION)
        self.assertEqual(len(curated), 1)
        self.assertEqual(report["equivalent_duplicate_groups"], 1)
        self.assertEqual(quarantine.iloc[0]["reason_code"], "EQUIVALENT_DUPLICATE")
        self.assertAlmostEqual(curated.iloc[0]["pace_seconds_per_kilometer"], 1740 / 8.007)
        self.assertEqual(curated.iloc[0]["quality_status"], "valid")

    def test_core_duplicate_conflict_quarantines_entire_activity(self) -> None:
        raw = pd.DataFrame(
            [
                activity_row(**{"Athlete ID": 123, "Serial": 1}),
                activity_row(**{"Athlete ID": 999, "Serial": 2}),
            ]
        )
        curated, quarantine, report = canonicalize_activities(raw, self.accounts, VALIDATION)
        self.assertTrue(curated.empty)
        self.assertEqual(len(quarantine), 2)
        self.assertEqual(report["conflicting_duplicate_groups"], 1)

    def test_window_identity_and_duration_guards(self) -> None:
        raw = pd.DataFrame(
            [
                activity_row(**{"Activity ID": 1, "Start Date": "2025-01-01 00:00:00+00:00"}),
                activity_row(**{"Activity ID": 2, "Athlete ID": 999}),
                activity_row(**{"Activity ID": 3, "Elapsed Time": 700000}),
            ]
        )
        curated, quarantine, report = canonicalize_activities(raw, self.accounts, VALIDATION)
        self.assertTrue(curated.empty)
        self.assertEqual(
            set(quarantine["reason_code"]),
            {"OUTSIDE_SNAPSHOT_WINDOW", "UNRESOLVED_EXTERNAL_ACCOUNT", "INVALID_ELAPSED_DURATION"},
        )

    def test_legacy_swim_distance_is_suppressed(self) -> None:
        raw = pd.DataFrame([activity_row(Type="Swim", **{"Distance (km)": 1500})])
        curated, _, report = canonicalize_activities(raw, self.accounts, VALIDATION)
        self.assertIsNone(curated.iloc[0]["distance_meters"])
        self.assertIn("AMBIGUOUS_SWIM_DISTANCE_UNITS", curated.iloc[0]["quality_flags"])
        self.assertEqual(report["swim_distance_values_suppressed_due_to_ambiguous_units"], 1)


class PerformanceCurationTests(unittest.TestCase):
    def test_mark_parser_supports_hand_timing_suffix(self) -> None:
        self.assertEqual(parse_performance_mark("2:03:04"), 7384)
        self.assertEqual(parse_performance_mark("3:39.3h"), 219.3)

    def test_performance_is_typed_and_linked(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "Mark": "3:39.3h",
                    "Competitor": "Runner ONE",
                    "Nat": "CAN",
                    "Location": "Track",
                    "Date": "03 JAN 2024",
                    "Results Score": 1101,
                    "Discipline": "1500m",
                    "Gender": "M",
                }
            ]
        )
        athletes = pd.DataFrame(
            [{"official_name_normalized": "runnerone", "athlete_id": "athlete-1"}]
        )
        config = {
            "disciplines": {
                "1500m": {"canonical_name": "1500m", "group": "middle_distance"}
            }
        }
        curated, quarantine, report = canonicalize_performances(raw, athletes, config)
        self.assertTrue(quarantine.empty)
        self.assertEqual(len(curated), 1)
        self.assertEqual(curated.iloc[0]["mark_seconds"], 219.3)
        self.assertTrue(curated.iloc[0]["is_season_best"])
        self.assertTrue(curated.iloc[0]["is_primary_discipline"])
        self.assertEqual(report["curated_rows"], 1)

    def test_primary_discipline_tie_break_is_deterministic(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "Mark": mark,
                    "Competitor": "Runner ONE",
                    "Nat": "CAN",
                    "Location": location,
                    "Date": date,
                    "Results Score": score,
                    "Discipline": discipline,
                    "Gender": "M",
                }
                for mark, location, date, score, discipline in [
                    ("3:40.0", "A", "01 JAN 2024", 1100, "1500m"),
                    ("3:41.0", "B", "02 JAN 2024", 1090, "1500m"),
                    ("13:20.0", "C", "03 JAN 2024", 1100, "5000m"),
                ]
            ]
        )
        athletes = pd.DataFrame(
            [{"official_name_normalized": "runnerone", "athlete_id": "athlete-1"}]
        )
        config = {
            "disciplines": {
                "1500m": {"canonical_name": "1500m", "group": "middle_distance"},
                "5000m": {"canonical_name": "5000m", "group": "track_distance"},
            }
        }
        curated, _, _ = canonicalize_performances(raw, athletes, config)
        self.assertEqual(
            set(curated.loc[curated["is_primary_discipline"], "discipline"]),
            {"1500m"},
        )


if __name__ == "__main__":
    unittest.main()
