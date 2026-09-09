"""Assertion-based quality checks for an EnduranceViz 2024 build."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from enduranceviz.analytics import reconciliation_report


def _check(name: str, passed: bool, detail: str) -> dict[str, Any]:
    return {"name": name, "status": "pass" if passed else "fail", "detail": detail}


def validate_frames(
    athletes: pd.DataFrame,
    accounts: pd.DataFrame,
    performances: pd.DataFrame,
    activities: pd.DataFrame,
    coverage: pd.DataFrame,
    weekly: pd.DataFrame,
    summaries: pd.DataFrame,
    contract: dict[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    athlete_ids = set(athletes["athlete_id"])
    account_keys = set(zip(accounts["provider"], accounts["external_account_id"]))
    timestamps = pd.to_datetime(activities["start_at_utc"], utc=True, errors="coerce")
    start = pd.Timestamp(contract["window"]["start_inclusive_utc"])
    end = pd.Timestamp(contract["window"]["end_exclusive_utc"])
    checks.extend(
        [
            _check(
                "unique_activity_ids",
                not activities["activity_id"].duplicated().any(),
                f"{activities['activity_id'].nunique()} unique IDs across {len(activities)} rows",
            ),
            _check(
                "unique_external_accounts",
                not accounts.duplicated(["provider", "external_account_id"]).any(),
                f"{len(account_keys)} unique provider/account keys across {len(accounts)} rows",
            ),
            _check(
                "activity_athlete_foreign_keys",
                set(activities["athlete_id"]).issubset(athlete_ids),
                "Every activity athlete_id resolves to athletes",
            ),
            _check(
                "activity_account_foreign_keys",
                set(zip(activities["provider"], activities["external_account_id"])).issubset(account_keys),
                "Every activity provider/account pair resolves to athlete_external_accounts",
            ),
            _check(
                "performance_athlete_foreign_keys",
                set(performances["athlete_id"]).issubset(athlete_ids),
                "Every performance athlete_id resolves to athletes",
            ),
            _check(
                "required_timestamps_parse",
                timestamps.notna().all(),
                f"{int(timestamps.isna().sum())} unparseable activity timestamps",
            ),
            _check(
                "activity_snapshot_window",
                bool(((timestamps >= start) & (timestamps < end)).all()),
                "All activity starts are inside the half-open 2024 UTC window",
            ),
        ]
    )
    nonnegative_columns = ["distance_meters", "elapsed_seconds", "moving_seconds"]
    negative = {
        column: int((pd.to_numeric(activities[column], errors="coerce") < 0).sum())
        for column in nonnegative_columns
    }
    checks.append(_check("nonnegative_activity_measurements", not any(negative.values()), str(negative)))
    valid_units = (
        activities["activity_category"].isin({"Run", "Ride", "Swim", "Other"}).all()
        and activities.loc[activities["activity_category"] == "Swim", "distance_meters"].isna().all()
        and (
            activities.loc[activities["pace_seconds_per_kilometer"].notna(), "distance_meters"] > 0
        ).all()
    )
    checks.append(
        _check(
            "canonical_units_and_categories",
            bool(valid_units),
            "Canonical categories are valid, swim distance is suppressed, and pace has positive distance",
        )
    )
    checks.extend(
        [
            _check(
                "unique_coverage_keys",
                not coverage.duplicated(["athlete_id", "week_start_utc"]).any(),
                f"{len(coverage)} unique coverage athlete-weeks",
            ),
            _check(
                "unique_weekly_keys",
                not weekly.duplicated(["athlete_id", "week_start_utc"]).any(),
                f"{len(weekly)} unique analytical athlete-weeks",
            ),
            _check(
                "coverage_weekly_key_alignment",
                set(zip(coverage["athlete_id"], coverage["week_start_utc"]))
                == set(zip(weekly["athlete_id"], weekly["week_start_utc"])),
                "Coverage and weekly tables have identical keys",
            ),
            _check(
                "missing_is_not_zero",
                weekly.loc[
                    (weekly["observation_status"] != "observed")
                    & weekly["activity_count"].isna(),
                    "run_distance_meters",
                ].isna().all(),
                "Inactive missing/unknown weeks retain null analytical values",
            ),
        ]
    )
    reconciliation = reconciliation_report(activities, weekly, summaries)
    checks.append(
        _check(
            "activity_weekly_summary_reconciliation",
            reconciliation["activity_count_reconciles"]
            and reconciliation["run_distance_max_absolute_difference_meters"] <= 0.001,
            str(reconciliation),
        )
    )
    performance_representation_ok = (
        ~performances["discipline"].astype(str).str.contains("|", regex=False)
        & ~performances["mark_text"].astype(str).str.contains("|", regex=False)
    ).all()
    checks.append(
        _check(
            "normalized_performance_representation",
            bool(performance_representation_ok),
            "Canonical performance rows contain one discipline and mark per row",
        )
    )
    baseline = contract["quality_baseline"]
    counts = {
        "athlete_rows": len(athletes),
        "external_account_rows": len(accounts),
        "performance_rows": len(performances),
        "activity_rows": len(activities),
        "coverage_rows": len(coverage),
        "weekly_rows": len(weekly),
    }
    count_match = all(int(counts[key]) == int(baseline[key]) for key in counts)
    cohort_count = int(summaries["default_cohort_eligible"].sum())
    regression_ok = count_match and cohort_count >= int(baseline["minimum_default_cohort_athletes"])
    checks.append(
        _check(
            "row_count_and_coverage_regression",
            regression_ok,
            f"actual={counts}, expected={baseline}, default_cohort={cohort_count}",
        )
    )
    failures = [check for check in checks if check["status"] == "fail"]
    return {
        "dataset": contract["dataset"]["name"],
        "specification_version": str(contract["dataset"]["specification_version"]),
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
            "+00:00", "Z"
        ),
        "status": "pass" if not failures else "fail",
        "check_count": len(checks),
        "failure_count": len(failures),
        "checks": checks,
    }


def markdown_report(report: dict[str, Any]) -> str:
    lines = [
        "# EnduranceViz 2024 data-quality report",
        "",
        f"- Dataset: `{report['dataset']}`",
        f"- Specification: `{report['specification_version']}`",
        f"- Generated: `{report['generated_at_utc']}`",
        f"- Result: **{report['status'].upper()}** ({report['check_count']} checks, {report['failure_count']} failures)",
        "",
        "| Check | Result | Detail |",
        "| --- | --- | --- |",
    ]
    for check in report["checks"]:
        detail = str(check["detail"]).replace("|", "\\|").replace("\n", " ")
        lines.append(f"| `{check['name']}` | {check['status'].upper()} | {detail} |")
    return "\n".join(lines) + "\n"
