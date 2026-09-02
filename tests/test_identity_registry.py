from __future__ import annotations

import unittest
import uuid
from datetime import datetime, timezone

import pandas as pd

from scripts.build_identity_registry import build_registries, normalize_name


class IdentityRegistryTests(unittest.TestCase):
    def test_name_normalization_is_for_matching_only(self) -> None:
        self.assertEqual(normalize_name("  Mário  García-Romo "), "mariogarciaromo")
        self.assertEqual(normalize_name("MARIO GARCÍA ROMO"), "mariogarciaromo")

    def test_zero_and_additional_accounts_resolve_without_using_row_order(self) -> None:
        performances = pd.DataFrame(
            [
                {
                    "Competitor": "Jack RAYNER",
                    "Nat": "AUS",
                    "Gender": "M",
                    "Athlete ID": None,
                }
            ]
        )
        metadata = pd.DataFrame(
            [
                {
                    "Athlete Name": "JACK RAYNER",
                    "Competitor": "Jack Rayner",
                    "Nat": "AUS",
                    "Gender": "Male",
                    "Athlete ID": 0,
                },
                {
                    "Athlete Name": "MARIO GARCÍA ROMO",
                    "Competitor": None,
                    "Nat": None,
                    "Gender": None,
                    "Athlete ID": 0,
                },
            ]
        )
        activities = pd.DataFrame(
            [
                {"Athlete ID": 10, "Athlete Name": "Jack Rayner"},
                {"Athlete ID": 11, "Athlete Name": "Jack Rayner"},
                {"Athlete ID": 20, "Athlete Name": "Mario García Romo"},
            ]
        )
        overrides = {
            "version": 1,
            "additional_athletes": [
                {
                    "metadata_display_name": "MARIO GARCÍA ROMO",
                    "official_name": "Mario García Romo",
                    "nationality_code": "ESP",
                    "gender": "male",
                    "identity_source": "repository_override",
                }
            ],
            "policies": [],
        }
        next_uuid = iter(
            [
                uuid.UUID("00000000-0000-0000-0000-000000000001"),
                uuid.UUID("00000000-0000-0000-0000-000000000002"),
            ]
        )

        athletes, accounts, report = build_registries(
            performances,
            metadata,
            activities,
            overrides,
            datetime(2026, 9, 2, tzinfo=timezone.utc),
            uuid_factory=lambda: next(next_uuid),
        )

        self.assertEqual(len(athletes), 2)
        self.assertEqual(len(accounts), 3)
        jack_accounts = accounts.loc[
            accounts["provider_display_name"] == "Jack Rayner", "athlete_id"
        ]
        self.assertEqual(jack_accounts.nunique(), 1)
        self.assertEqual(report["unresolved_activity_account_count"], 0)


if __name__ == "__main__":
    unittest.main()
