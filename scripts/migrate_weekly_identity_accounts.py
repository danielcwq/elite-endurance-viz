#!/usr/bin/env python3
"""Extend the persistent account registry using audited weekly batch evidence."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_identity_registry import (
    extend_accounts_with_weekly_evidence,
    load_weekly_account_evidence,
)


def main() -> int:
    reference = ROOT / "data" / "reference"
    athlete_path = reference / "athlete_registry_2024.csv"
    account_path = reference / "athlete_external_accounts_2024.csv"
    report_path = reference / "identity_resolution_report_2024.json"
    athletes = pd.read_csv(athlete_path, dtype=str)
    accounts = pd.read_csv(
        account_path,
        dtype={"athlete_id": str, "external_account_id": str},
    )
    extended, audit = extend_accounts_with_weekly_evidence(
        athletes,
        accounts,
        load_weekly_account_evidence(ROOT / "data" / "raw_data"),
    )
    if len(extended) == len(accounts):
        print("Account registry already includes all weekly evidence")
        return 0

    report = json.loads(report_path.read_text(encoding="utf-8"))
    report.update(audit)
    report["external_account_count"] = len(extended)
    temporary_accounts = account_path.with_suffix(".csv.tmp")
    temporary_report = report_path.with_suffix(".json.tmp")
    extended.to_csv(temporary_accounts, index=False, lineterminator="\n")
    temporary_report.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temporary_accounts.replace(account_path)
    temporary_report.replace(report_path)
    print(f"Extended external-account registry from {len(accounts)} to {len(extended)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
