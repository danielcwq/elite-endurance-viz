"""Coverage-aware weekly and athlete-level analytics for the 2024 snapshot."""

from __future__ import annotations

import re
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from scripts.build_identity_registry import normalize_external_id


WEEK_STARTS = [date(2024, 1, 1) + timedelta(days=7 * index) for index in range(53)]
FULL_WEEK_STARTS = set(WEEK_STARTS[:52])
DATE_RANGE_PATTERN = re.compile(
    r"Activities for (\d{1,2} [A-Za-z]{3} \d{4}) - (\d{1,2} [A-Za-z]{3} \d{4})"
)
TEMP_TIMESTAMP_PATTERN = re.compile(r"_(\d{8})_(\d{6})\.csv$")


def _collection_time(path: Path) -> pd.Timestamp | None:
    match = TEMP_TIMESTAMP_PATTERN.search(path.name)
    if not match:
        return None
    return pd.Timestamp(
        datetime.strptime("".join(match.groups()), "%Y%m%d%H%M%S"), tz="UTC"
    )


def load_weekly_evidence(paths: Iterable[Path], root: Path) -> pd.DataFrame:
    """Load repository weekly summaries as collection evidence, not metric truth."""
    frames: list[pd.DataFrame] = []
    required = {"Athlete ID", "Name", "Week Number", "Date Range"}
    for path in sorted(paths):
        if "indiv_activities" in path.name:
            continue
        frame = pd.read_csv(path, low_memory=False)
        if not required.issubset(frame.columns):
            continue
        frame = frame.copy()
        frame["external_account_id"] = frame["Athlete ID"].map(normalize_external_id)
        frame["week_number"] = pd.to_numeric(frame["Week Number"], errors="coerce").astype("Int64")
        frame["week_start_utc"] = frame["week_number"].map(
            lambda value: WEEK_STARTS[int(value) - 1]
            if pd.notna(value) and 1 <= int(value) <= 53
            else None
        )
        frame["source_file"] = path.relative_to(root).as_posix()
        frame["source_row_number"] = frame.index + 2
        frame["source_priority"] = 2 if path.parent.name == "tempdata" else 1
        frame["collection_completed_at_utc"] = _collection_time(path)
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    evidence = pd.concat(frames, ignore_index=True)
    return evidence.loc[
        evidence["external_account_id"].notna() & evidence["week_start_utc"].notna()
    ].reset_index(drop=True)


def _range_matches_week(value: Any, week_start: date) -> bool:
    if value is None or pd.isna(value):
        return False
    text = str(value).strip()
    if re.fullmatch(r"Week \d+ - No Data", text):
        return True
    match = DATE_RANGE_PATTERN.fullmatch(text)
    if not match:
        return False
    start = datetime.strptime(match.group(1), "%d %b %Y").date()
    end = datetime.strptime(match.group(2), "%d %b %Y").date()
    return start == week_start and end == week_start + timedelta(days=6)


def _evidence_signature(row: pd.Series) -> tuple[str, str, str, str]:
    return tuple(
        "" if pd.isna(row.get(column)) else str(row.get(column)).strip()
        for column in ("Date Range", "Distance (km)", "Time", "Elevation (m)")
    )


def select_weekly_evidence(evidence: pd.DataFrame) -> pd.DataFrame:
    """Choose one auditable row per account/week while retaining conflict flags."""
    rows: list[dict[str, Any]] = []
    for (account_id, week_start), group in evidence.groupby(
        ["external_account_id", "week_start_utc"], sort=True
    ):
        ordered = group.sort_values(
            ["source_priority", "collection_completed_at_utc", "source_file", "source_row_number"],
            ascending=[False, False, False, False],
            na_position="last",
            kind="stable",
        )
        selected = ordered.iloc[0]
        signatures = {_evidence_signature(row) for _, row in group.iterrows()}
        errors: list[str] = []
        if not _range_matches_week(selected.get("Date Range"), week_start):
            errors.append("WEEK_RANGE_MISMATCH")
        if len(signatures) > 1:
            errors.append("CONFLICTING_WEEKLY_SOURCE_ROWS")
        rows.append(
            {
                "external_account_id": account_id,
                "week_start_utc": week_start,
                "reports_no_data": bool(
                    re.fullmatch(r"Week \d+ - No Data", str(selected.get("Date Range", "")).strip())
                ),
                "collection_completed_at_utc": selected.get("collection_completed_at_utc"),
                "evidence_source": selected["source_file"],
                "evidence_source_count": int(len(group)),
                "evidence_error_codes": errors,
            }
        )
    return pd.DataFrame(rows)


def build_coverage(
    athletes: pd.DataFrame,
    accounts: pd.DataFrame,
    activities: pd.DataFrame,
    selected_evidence: pd.DataFrame,
) -> pd.DataFrame:
    account_to_athlete = accounts.set_index("external_account_id")["athlete_id"].to_dict()
    evidence = selected_evidence.copy()
    evidence["athlete_id"] = evidence["external_account_id"].map(account_to_athlete)
    if evidence["athlete_id"].isna().any():
        missing = sorted(evidence.loc[evidence["athlete_id"].isna(), "external_account_id"].unique())
        raise ValueError(f"Weekly evidence has unresolved external accounts: {missing}")

    activity_counts = activities.groupby(["athlete_id", "week_start_utc"])["activity_id"].nunique()
    accounts_by_athlete = accounts.groupby("athlete_id")["external_account_id"].nunique().to_dict()
    evidence_groups = {
        key: group for key, group in evidence.groupby(["athlete_id", "week_start_utc"], sort=False)
    }
    rows: list[dict[str, Any]] = []
    for athlete_id in athletes["athlete_id"]:
        has_account = athlete_id in accounts_by_athlete
        for week_start in WEEK_STARTS:
            activity_count = int(activity_counts.get((athlete_id, week_start), 0))
            group = evidence_groups.get((athlete_id, week_start))
            errors: set[str] = set()
            if group is not None:
                for values in group["evidence_error_codes"]:
                    errors.update(values)
                no_data = bool(group["reports_no_data"].all())
                if no_data and activity_count:
                    errors.add("SUMMARY_ACTIVITY_CONTRADICTION")
                if not no_data and not activity_count:
                    errors.add("SUMMARY_ACTIVITY_CONTRADICTION")
                observation_status = "observed"
                source = "|".join(sorted(group["evidence_source"].unique()))
                completed = pd.to_datetime(
                    group["collection_completed_at_utc"], utc=True, errors="coerce"
                ).max()
                completed = None if pd.isna(completed) else completed
                evidence_count = int(group["evidence_source_count"].sum())
            else:
                observation_status = "missing" if has_account else "unknown"
                source = None
                completed = None
                evidence_count = 0
            partial = week_start not in FULL_WEEK_STARTS
            complete = observation_status == "observed" and not errors and not partial
            if partial:
                coverage_status = "partial_window"
            elif complete:
                coverage_status = "complete"
            elif observation_status == "observed":
                coverage_status = "observed_with_warning"
            else:
                coverage_status = observation_status
            rows.append(
                {
                    "athlete_id": athlete_id,
                    "week_start_utc": week_start,
                    "observation_status": observation_status,
                    "is_active_week": activity_count > 0,
                    "is_complete_enough_week": complete,
                    "is_partial_window": partial,
                    "unique_activity_count": activity_count,
                    "coverage_status": coverage_status,
                    "collection_completed_at_utc": completed,
                    "collection_error_code": "|".join(sorted(errors)) or None,
                    "evidence_source": source,
                    "evidence_source_count": evidence_count,
                }
            )
    coverage = pd.DataFrame(rows)
    if coverage.duplicated(["athlete_id", "week_start_utc"]).any():
        raise ValueError("Coverage is not unique at athlete-week grain")
    return coverage


def _duration(frame: pd.DataFrame) -> pd.Series:
    return frame["moving_seconds"].fillna(frame["elapsed_seconds"]).fillna(0.0)


def build_weekly_training(
    coverage: pd.DataFrame,
    activities: pd.DataFrame,
    strength_types: set[str],
    rolling_window_weeks: int = 4,
) -> pd.DataFrame:
    activity = activities.copy()
    activity["effective_duration"] = _duration(activity)
    activity["activity_date_utc"] = pd.to_datetime(activity["start_at_utc"], utc=True).dt.date
    aggregated: dict[tuple[str, date], dict[str, Any]] = {}
    for key, group in activity.groupby(["athlete_id", "week_start_utc"], sort=False):
        categories = group["activity_category"]
        run = group.loc[categories == "Run"]
        ride = group.loc[categories == "Ride"]
        swim = group.loc[categories == "Swim"]
        strength = group.loc[group["provider_activity_type"].isin(strength_types)]
        other_cross = group.loc[
            (categories == "Other") & ~group["provider_activity_type"].isin(strength_types)
        ]
        counts_by_day = group.groupby("activity_date_utc")["activity_id"].nunique()
        run_distance = float(run["distance_meters"].fillna(0).sum())
        total_duration = float(group["effective_duration"].sum())
        cross_duration = float(group.loc[categories != "Run", "effective_duration"].sum())
        longest = float(run["distance_meters"].max()) if run["distance_meters"].notna().any() else None
        aggregated[key] = {
            "activity_count": int(group["activity_id"].nunique()),
            "active_days": int(len(counts_by_day)),
            "double_session_days": int((counts_by_day >= 2).sum()),
            "run_count": int(run["activity_id"].nunique()),
            "run_distance_meters": run_distance,
            "run_duration_seconds": float(run["effective_duration"].sum()),
            "longest_run_meters": longest,
            "long_run_share": longest / run_distance if longest is not None and run_distance > 0 else None,
            "ride_count": int(ride["activity_id"].nunique()),
            "ride_distance_meters": float(ride["distance_meters"].fillna(0).sum()),
            "ride_duration_seconds": float(ride["effective_duration"].sum()),
            "swim_count": int(swim["activity_id"].nunique()),
            "swim_distance_meters": None,
            "swim_duration_seconds": float(swim["effective_duration"].sum()),
            "strength_count": int(strength["activity_id"].nunique()),
            "strength_duration_seconds": float(strength["effective_duration"].sum()),
            "other_count": int(other_cross["activity_id"].nunique()),
            "other_cross_training_duration_seconds": float(other_cross["effective_duration"].sum()),
            "total_training_duration_seconds": total_duration,
            "cross_training_share": cross_duration / total_duration if total_duration > 0 else None,
        }

    metric_columns = [
        "activity_count", "active_days", "double_session_days", "run_count",
        "run_distance_meters", "run_duration_seconds", "longest_run_meters", "long_run_share",
        "ride_count", "ride_distance_meters", "ride_duration_seconds", "swim_count",
        "swim_distance_meters", "swim_duration_seconds", "strength_count",
        "strength_duration_seconds", "other_count", "other_cross_training_duration_seconds",
        "total_training_duration_seconds", "cross_training_share",
    ]
    count_columns = {"activity_count", "active_days", "double_session_days", "run_count", "ride_count", "swim_count", "strength_count", "other_count"}
    rows: list[dict[str, Any]] = []
    for row in coverage.to_dict("records"):
        key = (row["athlete_id"], row["week_start_utc"])
        metrics = aggregated.get(key)
        if metrics is None and row["observation_status"] == "observed":
            metrics = {column: 0 if column in count_columns or column not in {"longest_run_meters", "long_run_share", "swim_distance_meters", "cross_training_share"} else None for column in metric_columns}
        output = {
            "athlete_id": row["athlete_id"],
            "week_start_utc": row["week_start_utc"],
            "observation_status": row["observation_status"],
            "coverage_status": row["coverage_status"],
            "is_complete_enough_week": row["is_complete_enough_week"],
        }
        output.update(metrics or {column: None for column in metric_columns})
        rows.append(output)
    weekly = pd.DataFrame(rows).sort_values(["athlete_id", "week_start_utc"]).reset_index(drop=True)
    weekly["rolling_4w_observed_weeks"] = 0
    weekly["rolling_4w_run_distance_meters"] = np.nan
    weekly["rolling_4w_average_run_distance_meters"] = np.nan
    weekly["week_over_week_run_distance_change_meters"] = np.nan
    weekly["week_over_week_run_distance_change_fraction"] = np.nan
    for _, indexes in weekly.groupby("athlete_id", sort=False).groups.items():
        index_list = list(indexes)
        for offset, index in enumerate(index_list):
            start = max(0, offset - rolling_window_weeks + 1)
            window = weekly.loc[index_list[start : offset + 1]]
            observed_count = int(window["is_complete_enough_week"].sum())
            weekly.at[index, "rolling_4w_observed_weeks"] = observed_count
            if len(window) == rolling_window_weeks and observed_count == rolling_window_weeks:
                total = float(window["run_distance_meters"].sum())
                weekly.at[index, "rolling_4w_run_distance_meters"] = total
                weekly.at[index, "rolling_4w_average_run_distance_meters"] = total / rolling_window_weeks
            if offset:
                previous = weekly.loc[index_list[offset - 1]]
                current = weekly.loc[index]
                if bool(previous["is_complete_enough_week"]) and bool(current["is_complete_enough_week"]):
                    change = float(current["run_distance_meters"] - previous["run_distance_meters"])
                    weekly.at[index, "week_over_week_run_distance_change_meters"] = change
                    if float(previous["run_distance_meters"]) > 0:
                        weekly.at[index, "week_over_week_run_distance_change_fraction"] = change / float(previous["run_distance_meters"])
    return weekly


def _coverage_class(score: float, has_evidence: bool, thresholds: dict[str, Any]) -> str:
    if not has_evidence:
        return "unknown"
    if score >= float(thresholds["high_minimum"]):
        return "high"
    if score >= float(thresholds["moderate_minimum"]):
        return "moderate"
    if score >= float(thresholds["low_minimum"]):
        return "low"
    return "insufficient"


def build_athlete_summaries(
    athletes: pd.DataFrame,
    coverage: pd.DataFrame,
    weekly: pd.DataFrame,
    activities: pd.DataFrame,
    contract: dict[str, Any],
    computed_at: datetime,
) -> pd.DataFrame:
    config = contract["analytics"]["coverage_score"]
    full_week_count = int(contract["analytics"]["full_calendar_weeks"])
    calendar_week_equivalents = 366 / 7
    summaries: list[dict[str, Any]] = []
    for athlete_id in athletes["athlete_id"]:
        cov = coverage.loc[(coverage["athlete_id"] == athlete_id) & ~coverage["is_partial_window"]]
        all_weeks = weekly.loc[weekly["athlete_id"] == athlete_id]
        weeks = all_weeks.loc[all_weeks["week_start_utc"].isin(FULL_WEEK_STARTS)]
        activity = activities.loc[activities["athlete_id"] == athlete_id]
        observed = int((cov["observation_status"] == "observed").sum())
        complete = int(cov["is_complete_enough_week"].sum())
        breadth = observed / full_week_count
        consistency = complete / observed if observed else 0.0
        score = 100 * (
            float(config["observed_breadth_weight"]) * breadth
            + float(config["evidence_consistency_weight"]) * consistency
        )
        status = _coverage_class(score, observed > 0, config)
        observed_metrics = weeks.loc[weeks["observation_status"] == "observed"]
        run_activity = activity.loc[activity["activity_category"] == "Run"].copy()
        run_duration = _duration(run_activity)
        run_distance = float(run_activity["distance_meters"].fillna(0).sum())
        run_duration_total = float(run_duration.sum())
        observed_run_distance = float(observed_metrics["run_distance_meters"].fillna(0).sum())
        observed_run_duration = float(observed_metrics["run_duration_seconds"].fillna(0).sum())
        total_training = float(_duration(activity).sum())
        non_run = activity.loc[activity["activity_category"] != "Run"]
        cross_duration = float(_duration(non_run).sum())
        strength_types = set(contract["analytics"]["strength_activity_types"])
        strength_duration = float(_duration(activity.loc[activity["provider_activity_type"].isin(strength_types)]).sum())
        other_cross_duration = float(weeks["other_cross_training_duration_seconds"].fillna(0).sum())
        first = pd.to_datetime(activity["start_at_utc"], utc=True).min() if len(activity) else None
        last = pd.to_datetime(activity["start_at_utc"], utc=True).max() if len(activity) else None
        mean = float(observed_metrics["run_distance_meters"].mean()) if len(observed_metrics) else None
        std = float(observed_metrics["run_distance_meters"].std(ddof=0)) if len(observed_metrics) else None
        cv = std / mean if mean and std is not None else None
        weighted_pace = run_duration_total / (run_distance / 1000) if run_distance > 0 else None
        summaries.append(
            {
                "athlete_id": athlete_id,
                "coverage_status": status,
                "coverage_score": score,
                "default_cohort_eligible": status in {"high", "moderate"},
                "observed_weeks": observed,
                "missing_weeks": int((cov["observation_status"] == "missing").sum()),
                "active_weeks": int(cov["is_active_week"].sum()),
                "complete_enough_weeks": complete,
                "collection_gap_weeks": int(((cov["observation_status"] != "observed") & cov["is_active_week"]).sum()),
                "first_activity_at_utc": first,
                "last_activity_at_utc": last,
                "total_activity_count": int(activity["activity_id"].nunique()),
                "total_run_count": int(run_activity["activity_id"].nunique()),
                "total_run_distance_meters": run_distance,
                "total_run_duration_seconds": run_duration_total,
                "average_run_distance_per_observed_week_meters": observed_run_distance / observed if observed else None,
                "average_run_distance_per_calendar_week_meters": run_distance / calendar_week_equivalents,
                "average_run_duration_per_observed_week_seconds": observed_run_duration / observed if observed else None,
                "median_weekly_run_distance_meters": float(observed_metrics["run_distance_meters"].median()) if len(observed_metrics) else None,
                "peak_weekly_run_distance_meters": float(observed_metrics["run_distance_meters"].max()) if len(observed_metrics) else None,
                "peak_four_week_average_run_distance_meters": float(weeks["rolling_4w_average_run_distance_meters"].max()) if weeks["rolling_4w_average_run_distance_meters"].notna().any() else None,
                "weekly_run_distance_stddev_meters": std,
                "weekly_run_distance_cv": cv,
                "weekly_run_distance_consistency_score": 1 / (1 + cv) if cv is not None else None,
                "total_active_days": int(all_weeks["active_days"].fillna(0).sum()),
                "active_day_frequency_per_observed_week": float(observed_metrics["active_days"].fillna(0).sum()) / observed if observed else None,
                "longest_run_meters": float(run_activity["distance_meters"].max()) if run_activity["distance_meters"].notna().any() else None,
                "median_longest_run_meters": float(observed_metrics["longest_run_meters"].median()) if observed_metrics["longest_run_meters"].notna().any() else None,
                "average_long_run_share": float(observed_metrics["long_run_share"].mean()) if observed_metrics["long_run_share"].notna().any() else None,
                "total_ride_duration_seconds": float(all_weeks["ride_duration_seconds"].fillna(0).sum()),
                "total_swim_duration_seconds": float(all_weeks["swim_duration_seconds"].fillna(0).sum()),
                "total_strength_duration_seconds": strength_duration,
                "total_other_cross_training_duration_seconds": other_cross_duration,
                "total_training_duration_seconds": total_training,
                "cross_training_share": cross_duration / total_training if total_training > 0 else None,
                "weighted_run_pace_seconds_per_kilometer": weighted_pace,
                "metric_window_start_utc": pd.Timestamp(contract["window"]["start_inclusive_utc"]),
                "metric_window_end_exclusive_utc": pd.Timestamp(contract["window"]["end_exclusive_utc"]),
                "weekly_average_denominator": f"{observed} observed full UTC weeks; calendar average uses 366/7 week-equivalents",
                "dataset_name": contract["dataset"]["name"],
                "dataset_version": str(contract["dataset"]["specification_version"]),
                "computed_at_utc": computed_at,
            }
        )
    return pd.DataFrame(summaries)


def reconciliation_report(
    activities: pd.DataFrame, weekly: pd.DataFrame, summaries: pd.DataFrame
) -> dict[str, Any]:
    activity_run = activities.loc[activities["activity_category"] == "Run", "distance_meters"].fillna(0).sum()
    weekly_run = weekly["run_distance_meters"].fillna(0).sum()
    summary_run = summaries["total_run_distance_meters"].sum()
    activity_count = activities["activity_id"].nunique()
    weekly_count = int(weekly["activity_count"].fillna(0).sum())
    summary_count = int(summaries["total_activity_count"].sum())
    return {
        "activity_run_distance_meters": float(activity_run),
        "weekly_run_distance_meters": float(weekly_run),
        "summary_run_distance_meters": float(summary_run),
        "run_distance_max_absolute_difference_meters": float(
            max(abs(activity_run - weekly_run), abs(activity_run - summary_run))
        ),
        "activity_unique_count": int(activity_count),
        "weekly_activity_count": weekly_count,
        "summary_activity_count": summary_count,
        "activity_count_reconciles": activity_count == weekly_count == summary_count,
    }
