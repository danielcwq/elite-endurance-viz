"""Pure transformations for canonical 2024 activities and performances."""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from scripts.build_identity_registry import (
    normalize_external_id,
    normalize_gender,
    normalize_name,
    normalize_nationality,
)


CORE_DUPLICATE_FIELDS = ("Athlete ID", "Start Date", "Type", "Elapsed Time")
RUN_TYPES = {"Run", "TrailRun", "VirtualRun"}
RIDE_TYPES = {
    "Ride",
    "VirtualRide",
    "MountainBikeRide",
    "EMountainBikeRide",
    "EBikeRide",
    "GravelRide",
}
QUARANTINE_COLUMNS = (
    "quarantine_id",
    "entity_type",
    "source_file",
    "source_row_number",
    "source_record_json",
    "reason_code",
    "reason_detail",
)


def json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()
    return value


def source_record_json(row: pd.Series, excluded: set[str] | None = None) -> str:
    excluded = excluded or set()
    payload = {
        str(key): json_safe(value)
        for key, value in row.items()
        if key not in excluded and not str(key).startswith("_")
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def quarantine_row(
    entity_type: str,
    source_file: str,
    row: pd.Series,
    reason_code: str,
    reason_detail: str,
) -> dict[str, Any]:
    row_number = int(row["_source_row_number"])
    identifier = hashlib.sha256(
        f"{entity_type}|{source_file}|{row_number}|{reason_code}".encode("utf-8")
    ).hexdigest()
    return {
        "quarantine_id": identifier,
        "entity_type": entity_type,
        "source_file": source_file,
        "source_row_number": row_number,
        "source_record_json": source_record_json(row),
        "reason_code": reason_code,
        "reason_detail": reason_detail,
    }


def activity_category(raw_type: str) -> str:
    if raw_type in RUN_TYPES:
        return "Run"
    if raw_type in RIDE_TYPES:
        return "Ride"
    if raw_type == "Swim":
        return "Swim"
    return "Other"


def non_null_distinct(group: pd.DataFrame, column: str) -> set[str]:
    return {
        str(value)
        for value in group[column]
        if value is not None and not pd.isna(value)
    }


def distance_is_equivalent(
    group: pd.DataFrame,
    absolute_tolerance_meters: float,
    relative_tolerance: float,
) -> bool:
    values = pd.to_numeric(group["Distance (km)"], errors="coerce").dropna() * 1000
    if len(values) <= 1:
        return True
    difference = float(values.max() - values.min())
    tolerance = max(absolute_tolerance_meters, float(values.max()) * relative_tolerance)
    return difference <= tolerance


def positive_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    return number


def selection_score(row: pd.Series) -> tuple[int, int, int, int]:
    moving = positive_number(row.get("Activity Time (s)"))
    time_minutes = positive_number(row.get("Time (min)"))
    completeness_columns = (
        "Activity Name",
        "Description",
        "Location",
        "Distance (km)",
        "Activity Time (s)",
        "Time (min)",
    )
    completeness = sum(
        value is not None and not pd.isna(value) and str(value).strip() not in {"", "0", "0.0"}
        for value in (row.get(column) for column in completeness_columns)
    )
    distance = positive_number(row.get("Distance (km)"))
    has_sub_centimeter_precision = int(
        distance is not None and abs(distance * 100 - round(distance * 100)) > 1e-8
    )
    return (
        int(moving is not None or time_minutes is not None),
        completeness,
        has_sub_centimeter_precision,
        int(row["_source_row_number"]),
    )


def canonicalize_activities(
    raw: pd.DataFrame,
    accounts: pd.DataFrame,
    validation: dict[str, Any],
    source_file: str = "indiv_activities_full.csv",
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    working = raw.copy().reset_index(drop=True)
    if "_source_row_number" not in working:
        working["_source_row_number"] = working.index + 2
    if "_source_file" not in working:
        working["_source_file"] = source_file
    working["_activity_id"] = working["Activity ID"].map(normalize_external_id)
    working["_external_account_id"] = working["Athlete ID"].map(normalize_external_id)
    account_map = accounts.set_index("external_account_id")["athlete_id"].to_dict()

    duplicate_inflation_by_athlete = []
    for external_account_id, group in working.groupby("_external_account_id", dropna=False, sort=True):
        if external_account_id is None or pd.isna(external_account_id):
            continue
        raw_rows = len(group)
        unique_ids = int(group["_activity_id"].nunique())
        duplicate_inflation_by_athlete.append(
            {
                "athlete_id": account_map.get(external_account_id),
                "external_account_id": external_account_id,
                "raw_rows": raw_rows,
                "unique_activity_ids": unique_ids,
                "duplicate_extra_rows": raw_rows - unique_ids,
            }
        )

    quarantine: list[dict[str, Any]] = []
    retained_rows: list[pd.Series] = []
    duplicate_group_counts: Counter[str] = Counter()
    dropped_duplicate_rows = 0
    conflicting_group_count = 0

    for activity_id, group in working.groupby("_activity_id", dropna=False, sort=False):
        if activity_id is None or pd.isna(activity_id):
            for _, row in group.iterrows():
                quarantine.append(
                    quarantine_row(
                        "activity",
                        str(row["_source_file"]),
                        row,
                        "MISSING_ACTIVITY_ID",
                        "Activity ID is null",
                    )
                )
            continue

        if len(group) == 1:
            retained_rows.append(group.iloc[0])
            continue

        core_conflicts = [
            column for column in CORE_DUPLICATE_FIELDS if len(non_null_distinct(group, column)) > 1
        ]
        if core_conflicts or not distance_is_equivalent(
            group,
            float(validation["duplicate_distance_absolute_tolerance_meters"]),
            float(validation["duplicate_distance_relative_tolerance"]),
        ):
            conflicting_group_count += 1
            detail = (
                f"Conflicting duplicate fields: {core_conflicts or ['Distance (km) beyond tolerance']}"
            )
            for _, row in group.iterrows():
                quarantine.append(
                    quarantine_row(
                        "activity",
                        str(row["_source_file"]),
                        row,
                        "CONFLICTING_ACTIVITY_DUPLICATE",
                        detail,
                    )
                )
            continue

        comparison_columns = [
            column
            for column in raw.columns
            if column != "Serial" and not str(column).startswith("_")
        ]
        exact = len(group[comparison_columns].fillna("<NULL>").drop_duplicates()) == 1
        group_kind = "exact" if exact else "equivalent"
        duplicate_group_counts[group_kind] += 1
        selected_index = max(group.index, key=lambda index: selection_score(group.loc[index]))
        retained_rows.append(group.loc[selected_index])
        reason_code = "EXACT_DUPLICATE" if exact else "EQUIVALENT_DUPLICATE"
        for index, row in group.iterrows():
            if index == selected_index:
                continue
            dropped_duplicate_rows += 1
            quarantine.append(
                quarantine_row(
                    "activity",
                    str(row["_source_file"]),
                    row,
                    reason_code,
                    f"Retained source row {int(group.loc[selected_index, '_source_row_number'])}",
                )
            )

    canonical_rows: list[dict[str, Any]] = []
    validation_counts: Counter[str] = Counter()
    swim_distance_suppressed = 0
    for row in retained_rows:
        quality_flags: list[str] = []
        timestamp = pd.to_datetime(row.get("Start Date"), errors="coerce", utc=True)
        if pd.isna(timestamp):
            quarantine.append(
                quarantine_row(
                    "activity",
                    str(row["_source_file"]),
                    row,
                    "INVALID_START_TIMESTAMP",
                    "Start Date is not UTC-parseable",
                )
            )
            validation_counts["INVALID_START_TIMESTAMP"] += 1
            continue
        if not (pd.Timestamp("2024-01-01", tz="UTC") <= timestamp < pd.Timestamp("2025-01-01", tz="UTC")):
            quarantine.append(
                quarantine_row(
                    "activity",
                    str(row["_source_file"]),
                    row,
                    "OUTSIDE_SNAPSHOT_WINDOW",
                    str(timestamp),
                )
            )
            validation_counts["OUTSIDE_SNAPSHOT_WINDOW"] += 1
            continue

        external_account_id = row["_external_account_id"]
        athlete_id = account_map.get(external_account_id)
        if athlete_id is None:
            quarantine.append(
                quarantine_row(
                    "activity",
                    str(row["_source_file"]),
                    row,
                    "UNRESOLVED_EXTERNAL_ACCOUNT",
                    f"Strava account {external_account_id} is not in the persistent registry",
                )
            )
            validation_counts["UNRESOLVED_EXTERNAL_ACCOUNT"] += 1
            continue

        raw_type = None if pd.isna(row.get("Type")) else str(row.get("Type")).strip()
        if not raw_type:
            quarantine.append(
                quarantine_row(
                    "activity",
                    str(row["_source_file"]),
                    row,
                    "MISSING_ACTIVITY_TYPE",
                    "Type is empty",
                )
            )
            validation_counts["MISSING_ACTIVITY_TYPE"] += 1
            continue
        category = activity_category(raw_type)

        elapsed_seconds = positive_number(row.get("Elapsed Time"))
        if elapsed_seconds is None or elapsed_seconds > float(validation["maximum_elapsed_seconds"]):
            quarantine.append(
                quarantine_row(
                    "activity",
                    str(row["_source_file"]),
                    row,
                    "INVALID_ELAPSED_DURATION",
                    f"Elapsed seconds: {row.get('Elapsed Time')}",
                )
            )
            validation_counts["INVALID_ELAPSED_DURATION"] += 1
            continue

        moving_seconds = positive_number(row.get("Activity Time (s)"))
        if moving_seconds is None:
            time_minutes = positive_number(row.get("Time (min)"))
            moving_seconds = time_minutes * 60 if time_minutes is not None else None
        if moving_seconds is not None:
            tolerance = max(
                float(validation["moving_elapsed_absolute_tolerance_seconds"]),
                elapsed_seconds * float(validation["moving_elapsed_relative_tolerance"]),
            )
            if moving_seconds > elapsed_seconds + tolerance:
                quarantine.append(
                    quarantine_row(
                        "activity",
                        str(row["_source_file"]),
                        row,
                        "MOVING_TIME_EXCEEDS_ELAPSED",
                        f"Moving {moving_seconds}s exceeds elapsed {elapsed_seconds}s",
                    )
                )
                validation_counts["MOVING_TIME_EXCEEDS_ELAPSED"] += 1
                continue
            if moving_seconds > elapsed_seconds:
                quality_flags.append("MOVING_TIME_ROUNDING_ADJUSTED")
            moving_seconds = min(moving_seconds, elapsed_seconds)
        else:
            quality_flags.append("MISSING_MOVING_TIME")

        raw_distance_km = positive_number(row.get("Distance (km)"))
        distance_meters = raw_distance_km * 1000 if raw_distance_km is not None else None
        if category == "Run" and distance_meters is not None:
            if distance_meters > float(validation["maximum_run_distance_meters"]):
                quarantine.append(
                    quarantine_row(
                        "activity",
                        str(row["_source_file"]),
                        row,
                        "IMPOSSIBLE_RUN_DISTANCE",
                        f"Distance meters: {distance_meters}",
                    )
                )
                validation_counts["IMPOSSIBLE_RUN_DISTANCE"] += 1
                continue
        elif category == "Ride" and distance_meters is not None:
            if distance_meters > float(validation["maximum_ride_distance_meters"]):
                quarantine.append(
                    quarantine_row(
                        "activity",
                        str(row["_source_file"]),
                        row,
                        "IMPOSSIBLE_RIDE_DISTANCE",
                        f"Distance meters: {distance_meters}",
                    )
                )
                validation_counts["IMPOSSIBLE_RIDE_DISTANCE"] += 1
                continue
        elif category == "Swim":
            if distance_meters is not None:
                swim_distance_suppressed += 1
            quality_flags.append("AMBIGUOUS_SWIM_DISTANCE_UNITS")
            distance_meters = None
        else:
            distance_meters = None

        if category in {"Run", "Ride"} and distance_meters is None:
            quality_flags.append("MISSING_DISTANCE")

        pace_seconds_per_kilometer = None
        if moving_seconds is not None and distance_meters is not None and distance_meters > 0:
            pace_seconds_per_kilometer = moving_seconds / (distance_meters / 1000)

        monday = (timestamp - pd.Timedelta(days=timestamp.weekday())).date()
        canonical_rows.append(
            {
                "activity_id": row["_activity_id"],
                "athlete_id": athlete_id,
                "provider": "strava",
                "external_account_id": external_account_id,
                "provider_activity_type": raw_type,
                "activity_category": category,
                "activity_name": None if pd.isna(row.get("Activity Name")) else row.get("Activity Name"),
                "description": None if pd.isna(row.get("Description")) else row.get("Description"),
                "start_at_utc": timestamp,
                "week_start_utc": monday,
                "distance_meters": distance_meters,
                "elapsed_seconds": elapsed_seconds,
                "moving_seconds": moving_seconds,
                "pace_seconds_per_kilometer": pace_seconds_per_kilometer,
                "location": None if pd.isna(row.get("Location")) else row.get("Location"),
                "quality_status": "warning" if quality_flags else "valid",
                "quality_flags": quality_flags,
                "exclusion_reason": None,
                "source_file": str(row["_source_file"]),
                "source_row_number": int(row["_source_row_number"]),
            }
        )

    curated = pd.DataFrame(canonical_rows)
    if not curated.empty:
        curated = curated.sort_values(["activity_id"], kind="stable").reset_index(drop=True)
    quarantine_frame = pd.DataFrame(quarantine, columns=QUARANTINE_COLUMNS).sort_values(
        ["source_row_number", "reason_code"], kind="stable"
    ).reset_index(drop=True)
    source_files = sorted(str(value) for value in working["_source_file"].dropna().unique())
    report = {
        "source_file": source_files[0] if len(source_files) == 1 else "multiple_manifested_sources",
        "source_files": source_files,
        "input_rows": len(raw),
        "input_unique_activity_ids": int(working["_activity_id"].nunique()),
        "exact_duplicate_groups": duplicate_group_counts["exact"],
        "equivalent_duplicate_groups": duplicate_group_counts["equivalent"],
        "conflicting_duplicate_groups": conflicting_group_count,
        "dropped_duplicate_rows": dropped_duplicate_rows,
        "curated_rows": len(curated),
        "curated_unique_activity_ids": int(curated["activity_id"].nunique()) if not curated.empty else 0,
        "quarantine_rows": len(quarantine_frame),
        "validation_exclusions": dict(sorted(validation_counts.items())),
        "swim_distance_values_suppressed_due_to_ambiguous_units": swim_distance_suppressed,
        "duplicate_inflation_by_athlete": duplicate_inflation_by_athlete,
    }
    return curated, quarantine_frame, report


def parse_performance_mark(mark: Any) -> float:
    text = str(mark).strip()
    if text.endswith("h"):
        text = text[:-1]
    parts = text.split(":")
    try:
        numbers = [float(part) for part in parts]
    except ValueError as exc:
        raise ValueError(f"Unsupported performance mark: {mark}") from exc
    if len(numbers) == 3:
        return numbers[0] * 3600 + numbers[1] * 60 + numbers[2]
    if len(numbers) == 2:
        return numbers[0] * 60 + numbers[1]
    if len(numbers) == 1:
        return numbers[0]
    raise ValueError(f"Unsupported performance mark: {mark}")


def canonicalize_performances(
    raw: pd.DataFrame,
    athletes: pd.DataFrame,
    discipline_config: dict[str, Any],
    source_file: str = "data/metadata/master_iaaf_database_with_strava.csv",
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    working = raw.copy().reset_index(drop=True)
    working["_source_row_number"] = working.index + 2
    athlete_map = athletes.set_index("official_name_normalized")["athlete_id"].to_dict()
    disciplines = discipline_config["disciplines"]
    rows: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()

    for _, row in working.iterrows():
        identity_key = normalize_name(row.get("Competitor"))
        athlete_id = athlete_map.get(identity_key)
        if athlete_id is None:
            reason = "UNRESOLVED_PERFORMANCE_ATHLETE"
            quarantine.append(
                quarantine_row("performance", source_file, row, reason, str(row.get("Competitor")))
            )
            reason_counts[reason] += 1
            continue
        raw_discipline = str(row.get("Discipline")).strip()
        discipline = disciplines.get(raw_discipline)
        if discipline is None:
            reason = "UNKNOWN_DISCIPLINE"
            quarantine.append(
                quarantine_row("performance", source_file, row, reason, raw_discipline)
            )
            reason_counts[reason] += 1
            continue
        performance_date = pd.to_datetime(row.get("Date"), format="%d %b %Y", errors="coerce")
        if pd.isna(performance_date) or performance_date.year != 2024:
            reason = "INVALID_PERFORMANCE_DATE"
            quarantine.append(
                quarantine_row("performance", source_file, row, reason, str(row.get("Date")))
            )
            reason_counts[reason] += 1
            continue
        try:
            mark_seconds = parse_performance_mark(row.get("Mark"))
        except ValueError as exc:
            reason = "INVALID_PERFORMANCE_MARK"
            quarantine.append(
                quarantine_row("performance", source_file, row, reason, str(exc))
            )
            reason_counts[reason] += 1
            continue
        results_score = pd.to_numeric(row.get("Results Score"), errors="coerce")
        if pd.isna(results_score) or int(results_score) <= 0:
            reason = "INVALID_RESULTS_SCORE"
            quarantine.append(
                quarantine_row("performance", source_file, row, reason, str(row.get("Results Score")))
            )
            reason_counts[reason] += 1
            continue
        location = None if pd.isna(row.get("Location")) else str(row.get("Location")).strip()
        mark_text = str(row.get("Mark")).strip()
        natural_key = "|".join(
            [athlete_id, discipline["canonical_name"], mark_text, performance_date.date().isoformat(), location or ""]
        )
        rows.append(
            {
                "performance_id": hashlib.sha256(natural_key.encode("utf-8")).hexdigest(),
                "athlete_id": athlete_id,
                "discipline": discipline["canonical_name"],
                "discipline_group": discipline["group"],
                "mark_text": mark_text,
                "mark_seconds": mark_seconds,
                "mark_meters": None,
                "results_score": int(results_score),
                "performance_date": performance_date.date(),
                "location": location,
                "nationality_code": normalize_nationality(row.get("Nat")),
                "gender": normalize_gender(row.get("Gender")),
                "source_file": source_file,
                "source_row_number": int(row["_source_row_number"]),
            }
        )

    curated = pd.DataFrame(rows)
    if not curated.empty:
        duplicate_mask = curated.duplicated("performance_id", keep="first")
        if duplicate_mask.any():
            raise ValueError("Canonical performance IDs are unexpectedly duplicated")
        curated["is_season_best"] = curated["mark_seconds"].eq(
            curated.groupby(["athlete_id", "discipline"])["mark_seconds"].transform("min")
        )
        discipline_rank = (
            curated.groupby(["athlete_id", "discipline"], as_index=False)
            .agg(
                highest_results_score=("results_score", "max"),
                performance_count=("performance_id", "size"),
            )
            .sort_values(
                ["athlete_id", "highest_results_score", "performance_count", "discipline"],
                ascending=[True, False, False, True],
                kind="stable",
            )
        )
        primary_by_athlete = (
            discipline_rank.drop_duplicates("athlete_id")
            .set_index("athlete_id")["discipline"]
            .to_dict()
        )
        curated["is_primary_discipline"] = curated.apply(
            lambda row: primary_by_athlete[row["athlete_id"]] == row["discipline"],
            axis=1,
        )
        curated = curated.sort_values(["performance_id"], kind="stable").reset_index(drop=True)
    quarantine_frame = pd.DataFrame(quarantine, columns=QUARANTINE_COLUMNS)
    report = {
        "source_file": source_file,
        "input_rows": len(raw),
        "curated_rows": len(curated),
        "curated_unique_performance_ids": int(curated["performance_id"].nunique()) if not curated.empty else 0,
        "curated_athletes": int(curated["athlete_id"].nunique()) if not curated.empty else 0,
        "quarantine_rows": len(quarantine_frame),
        "quarantine_reasons": dict(sorted(reason_counts.items())),
        "discipline_counts": curated["discipline"].value_counts().sort_index().to_dict() if not curated.empty else {},
        "season_best_rows": int(curated["is_season_best"].sum()) if not curated.empty else 0,
        "primary_discipline_counts": (
            curated.loc[curated["is_primary_discipline"]]
            .drop_duplicates("athlete_id")["discipline"]
            .value_counts()
            .sort_index()
            .to_dict()
            if not curated.empty
            else {}
        ),
    }
    return curated, quarantine_frame, report
