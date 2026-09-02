#!/usr/bin/env python3
"""Bootstrap the persistent 2024 athlete and external-account registries."""

from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import yaml


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PERFORMANCES = ROOT / "data" / "metadata" / "master_iaaf_database_with_strava.csv"
DEFAULT_METADATA = ROOT / "cleaned_athlete_metadata.csv"
DEFAULT_ACTIVITIES = ROOT / "indiv_activities_full.csv"
DEFAULT_OVERRIDES = ROOT / "config" / "identity_overrides_2024.yaml"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "reference"


def normalize_name(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    decomposed = unicodedata.normalize("NFKD", str(value))
    ascii_text = decomposed.encode("ascii", "ignore").decode("ascii").casefold()
    return re.sub(r"[^a-z0-9]+", "", ascii_text)


def normalize_gender(value: Any) -> str:
    if value is None or pd.isna(value):
        return "unknown"
    normalized = str(value).strip().casefold()
    return {
        "f": "female",
        "female": "female",
        "m": "male",
        "male": "male",
        "other": "other",
    }.get(normalized, "unknown")


def normalize_external_id(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text == "0" or text.casefold() in {"nan", "none", "null"}:
        return None
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def normalize_nationality(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().upper()
    return text if len(text) == 3 else None


def most_common_text(values: pd.Series) -> str | None:
    cleaned = [str(value).strip() for value in values if not pd.isna(value) and str(value).strip()]
    if not cleaned:
        return None
    counts = Counter(cleaned)
    return sorted(counts, key=lambda value: (-counts[value], value.casefold(), value))[0]


def load_overrides(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if payload.get("version") != 1:
        raise ValueError(f"Unsupported identity override version in {path}")
    return payload


def make_athlete_candidates(
    performances: pd.DataFrame,
    metadata: pd.DataFrame,
    overrides: dict[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    candidates: dict[str, dict[str, Any]] = {}
    metadata_display_overrides: dict[str, str] = {}

    performance_copy = performances.copy()
    performance_copy["_identity_key"] = performance_copy["Competitor"].map(normalize_name)
    for identity_key, group in performance_copy[performance_copy["_identity_key"] != ""].groupby(
        "_identity_key", sort=True
    ):
        genders = {normalize_gender(value) for value in group["Gender"]}
        genders.discard("unknown")
        if len(genders) > 1:
            raise ValueError(f"Conflicting genders for normalized identity {identity_key}: {genders}")
        nationalities = {
            nationality
            for nationality in group["Nat"].map(normalize_nationality)
            if nationality is not None
        }
        candidates[identity_key] = {
            "official_name": most_common_text(group["Competitor"]),
            "nationality_code": next(iter(nationalities)) if len(nationalities) == 1 else None,
            "gender": next(iter(genders)) if genders else "unknown",
            "identity_source": "world_athletics",
            "nationality_candidates": sorted(nationalities),
        }

    metadata_copy = metadata.copy()
    metadata_copy["_identity_key"] = metadata_copy["Competitor"].map(normalize_name)
    for _, row in metadata_copy[metadata_copy["_identity_key"] != ""].iterrows():
        identity_key = row["_identity_key"]
        if identity_key not in candidates:
            raise ValueError(f"Metadata identity is absent from performance source: {row['Competitor']}")
        candidate = candidates[identity_key]
        candidate["official_name"] = str(row["Competitor"]).strip()
        nationality = normalize_nationality(row.get("Nat"))
        if nationality is not None:
            candidate["nationality_code"] = nationality
        gender = normalize_gender(row.get("Gender"))
        if gender != "unknown":
            candidate["gender"] = gender
        candidate["metadata_display_name"] = str(row["Athlete Name"]).strip()

    for override in overrides.get("additional_athletes", []):
        identity_key = normalize_name(override["official_name"])
        if not identity_key:
            raise ValueError("Additional athlete override has an empty normalized name")
        existing = candidates.get(identity_key)
        if existing is not None and existing["official_name"] != override["official_name"]:
            raise ValueError(f"Override collides with existing identity: {identity_key}")
        candidates[identity_key] = {
            "official_name": override["official_name"],
            "nationality_code": normalize_nationality(override.get("nationality_code")),
            "gender": normalize_gender(override.get("gender")),
            "identity_source": override.get("identity_source", "repository_override"),
            "nationality_candidates": [override["nationality_code"]],
            "metadata_display_name": override["metadata_display_name"],
        }
        metadata_display_overrides[
            normalize_name(override["metadata_display_name"])
        ] = identity_key

    return candidates, metadata_display_overrides


def build_registries(
    performances: pd.DataFrame,
    metadata: pd.DataFrame,
    activities: pd.DataFrame,
    overrides: dict[str, Any],
    generated_at: datetime,
    uuid_factory: Callable[[], uuid.UUID] = uuid.uuid4,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    candidates, metadata_display_overrides = make_athlete_candidates(
        performances, metadata, overrides
    )
    generated_iso = generated_at.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )

    athlete_rows: list[dict[str, Any]] = []
    identity_key_to_id: dict[str, str] = {}
    for identity_key in sorted(candidates):
        candidate = candidates[identity_key]
        athlete_id = str(uuid_factory())
        identity_key_to_id[identity_key] = athlete_id
        athlete_rows.append(
            {
                "athlete_id": athlete_id,
                "official_name": candidate["official_name"],
                "official_name_normalized": identity_key,
                "display_name": candidate.get("metadata_display_name"),
                "nationality_code": candidate.get("nationality_code"),
                "gender": candidate["gender"],
                "identity_status": "resolved",
                "identity_source": candidate["identity_source"],
                "created_at_utc": generated_iso,
                "updated_at_utc": generated_iso,
            }
        )

    metadata_copy = metadata.copy().reset_index(drop=True)
    metadata_copy["_official_key"] = metadata_copy["Competitor"].map(normalize_name)
    metadata_copy["_display_key"] = metadata_copy["Athlete Name"].map(normalize_name)
    metadata_copy["_external_id"] = metadata_copy["Athlete ID"].map(normalize_external_id)
    metadata_key_by_row: dict[int, str] = {}
    for index, row in metadata_copy.iterrows():
        identity_key = row["_official_key"] or metadata_display_overrides.get(row["_display_key"], "")
        if identity_key not in identity_key_to_id:
            raise ValueError(f"Unable to resolve metadata identity: {row['Athlete Name']}")
        metadata_key_by_row[index] = identity_key

    metadata_name_index: dict[str, set[str]] = defaultdict(set)
    for index, row in metadata_copy.iterrows():
        identity_key = metadata_key_by_row[index]
        for name_key in (row["_official_key"], row["_display_key"]):
            if name_key:
                metadata_name_index[name_key].add(identity_key)

    account_rows: dict[str, dict[str, Any]] = {}
    for index, row in metadata_copy[metadata_copy["_external_id"].notna()].iterrows():
        external_id = row["_external_id"]
        identity_key = metadata_key_by_row[index]
        if external_id in account_rows:
            raise ValueError(f"Duplicate external account in metadata: {external_id}")
        account_rows[external_id] = {
            "provider": "strava",
            "external_account_id": external_id,
            "athlete_id": identity_key_to_id[identity_key],
            "provider_display_name": str(row["Athlete Name"]).strip(),
            "match_method": "metadata_external_id",
            "match_status": "resolved",
            "source_file": "cleaned_athlete_metadata.csv",
            "source_row_number": index + 2,
        }

    activity_copy = activities.copy().reset_index(drop=True)
    activity_copy["_external_id"] = activity_copy["Athlete ID"].map(normalize_external_id)
    activity_copy["_display_key"] = activity_copy["Athlete Name"].map(normalize_name)
    activity_accounts_absent_metadata: list[dict[str, Any]] = []
    unresolved_accounts: list[dict[str, Any]] = []
    names_by_account: dict[str, Counter[str]] = defaultdict(Counter)
    first_row_by_account: dict[str, int] = {}

    for index, row in activity_copy.iterrows():
        external_id = row["_external_id"]
        if external_id is None:
            continue
        display_name = str(row["Athlete Name"]).strip()
        names_by_account[external_id][display_name] += 1
        first_row_by_account.setdefault(external_id, index + 2)

    for external_id in sorted(names_by_account, key=lambda value: (len(value), value)):
        display_counts = names_by_account[external_id]
        display_name = sorted(
            display_counts, key=lambda value: (-display_counts[value], value.casefold(), value)
        )[0]
        display_key = normalize_name(display_name)
        if external_id in account_rows:
            expected_athlete_id = account_rows[external_id]["athlete_id"]
            possible_keys = metadata_name_index.get(display_key, set())
            if possible_keys and expected_athlete_id not in {
                identity_key_to_id[key] for key in possible_keys
            }:
                raise ValueError(
                    f"Activity name conflicts with metadata account {external_id}: {display_name}"
                )
            account_rows[external_id]["provider_display_name"] = display_name
            continue

        possible_keys = metadata_name_index.get(display_key, set())
        if len(possible_keys) != 1:
            unresolved_accounts.append(
                {
                    "external_account_id": external_id,
                    "provider_display_name": display_name,
                    "candidate_identity_keys": sorted(possible_keys),
                }
            )
            continue
        identity_key = next(iter(possible_keys))
        account_rows[external_id] = {
            "provider": "strava",
            "external_account_id": external_id,
            "athlete_id": identity_key_to_id[identity_key],
            "provider_display_name": display_name,
            "match_method": "exact_normalized_name_to_metadata",
            "match_status": "resolved",
            "source_file": "indiv_activities_full.csv",
            "source_row_number": first_row_by_account[external_id],
        }
        activity_accounts_absent_metadata.append(
            {
                "external_account_id": external_id,
                "provider_display_name": display_name,
                "official_name": candidates[identity_key]["official_name"],
            }
        )

    if unresolved_accounts:
        raise ValueError(f"Unresolved activity accounts: {unresolved_accounts}")

    account_frame = pd.DataFrame(account_rows.values()).sort_values(
        ["provider", "external_account_id"], kind="stable"
    )
    if account_frame.duplicated(["provider", "external_account_id"]).any():
        raise ValueError("External account registry is not unique")

    activity_display_by_athlete: dict[str, Counter[str]] = defaultdict(Counter)
    account_to_athlete = account_frame.set_index("external_account_id")["athlete_id"].to_dict()
    for external_id, display_counts in names_by_account.items():
        athlete_id = account_to_athlete[external_id]
        activity_display_by_athlete[athlete_id].update(display_counts)
    for row in athlete_rows:
        display_counts = activity_display_by_athlete.get(row["athlete_id"])
        if display_counts:
            row["display_name"] = sorted(
                display_counts,
                key=lambda value: (-display_counts[value], value.casefold(), value),
            )[0]

    athlete_frame = pd.DataFrame(athlete_rows).sort_values(
        ["official_name_normalized"], kind="stable"
    )
    if athlete_frame["athlete_id"].duplicated().any():
        raise ValueError("Generated internal athlete UUIDs are not unique")
    if athlete_frame["official_name_normalized"].duplicated().any():
        raise ValueError("Canonical normalized athlete identities are not unique")

    performance_copy = performances.copy()
    performance_copy["_identity_key"] = performance_copy["Competitor"].map(normalize_name)
    performance_copy["_external_id"] = performance_copy["Athlete ID"].map(normalize_external_id)
    discarded_candidates = []
    for identity_key, group in performance_copy[performance_copy["_external_id"].notna()].groupby(
        "_identity_key", sort=True
    ):
        candidate_ids = sorted(set(group["_external_id"]), key=lambda value: (len(value), value))
        if len(candidate_ids) > 1:
            discarded_candidates.append(
                {
                    "official_name": candidates[identity_key]["official_name"],
                    "legacy_candidate_ids": candidate_ids,
                    "resolution": "ignored_performance_processing_fields",
                }
            )

    nationality_conflicts = [
        {
            "official_name": candidate["official_name"],
            "nationality_candidates": candidate["nationality_candidates"],
            "canonical_nationality": candidate["nationality_code"],
        }
        for candidate in candidates.values()
        if len(candidate.get("nationality_candidates", [])) > 1
    ]

    zero_id_resolutions = []
    for index, row in metadata_copy[metadata_copy["_external_id"].isna()].iterrows():
        identity_key = metadata_key_by_row[index]
        athlete_id = identity_key_to_id[identity_key]
        resolved_accounts = account_frame.loc[
            account_frame["athlete_id"] == athlete_id, "external_account_id"
        ].tolist()
        zero_id_resolutions.append(
            {
                "metadata_display_name": row["Athlete Name"],
                "resolved_external_account_ids": resolved_accounts,
            }
        )

    report = {
        "generated_at_utc": generated_iso,
        "athlete_count": len(athlete_frame),
        "world_athletics_identity_count": sum(
            athlete_frame["identity_source"] == "world_athletics"
        ),
        "repository_override_identity_count": sum(
            athlete_frame["identity_source"] == "repository_override"
        ),
        "external_account_count": len(account_frame),
        "unresolved_activity_account_count": 0,
        "zero_id_resolutions": zero_id_resolutions,
        "activity_accounts_absent_metadata": activity_accounts_absent_metadata,
        "discarded_multi_id_performance_candidates": discarded_candidates,
        "nationality_conflicts": nationality_conflicts,
        "policies": overrides.get("policies", []),
    }
    return athlete_frame, account_frame, report


def refuse_outputs(paths: list[Path]) -> None:
    existing = [str(path) for path in paths if path.exists()]
    if existing:
        raise FileExistsError(f"Refusing to overwrite persistent identity outputs: {existing}")


def write_outputs(
    athlete_frame: pd.DataFrame,
    account_frame: pd.DataFrame,
    report: dict[str, Any],
    output_dir: Path,
) -> None:
    athlete_path = output_dir / "athlete_registry_2024.csv"
    account_path = output_dir / "athlete_external_accounts_2024.csv"
    report_path = output_dir / "identity_resolution_report_2024.json"
    refuse_outputs([athlete_path, account_path, report_path])
    output_dir.mkdir(parents=True, exist_ok=True)
    athlete_frame.to_csv(athlete_path, index=False, lineterminator="\n")
    account_frame.to_csv(account_path, index=False, lineterminator="\n")
    report_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--performances", type=Path, default=DEFAULT_PERFORMANCES)
    parser.add_argument("--metadata", type=Path, default=DEFAULT_METADATA)
    parser.add_argument("--activities", type=Path, default=DEFAULT_ACTIVITIES)
    parser.add_argument("--overrides", type=Path, default=DEFAULT_OVERRIDES)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        performances = pd.read_csv(args.performances, low_memory=False)
        metadata = pd.read_csv(args.metadata, low_memory=False)
        activities = pd.read_csv(
            args.activities,
            usecols=["Athlete ID", "Athlete Name"],
            low_memory=False,
        )
        athletes, accounts, report = build_registries(
            performances,
            metadata,
            activities,
            load_overrides(args.overrides),
            datetime.now(timezone.utc),
        )
        write_outputs(athletes, accounts, report, args.output_dir)
    except (FileExistsError, OSError, ValueError, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"Wrote {len(athletes)} athletes and {len(accounts)} external accounts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
