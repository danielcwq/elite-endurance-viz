#!/usr/bin/env python3
"""Build canonical 2024 activity and performance Parquet tables."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from enduranceviz.curation import canonicalize_activities, canonicalize_performances


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def atomic_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temporary, index=False)
    temporary.replace(path)


def atomic_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def build(args: argparse.Namespace) -> dict[str, Any]:
    contract = load_yaml(args.contract)
    disciplines = load_yaml(args.disciplines)
    athletes = pd.read_csv(args.athletes, dtype={"athlete_id": str})
    accounts = pd.read_csv(
        args.accounts,
        dtype={"athlete_id": str, "external_account_id": str},
    )
    raw_activities = pd.read_csv(args.raw_activities, low_memory=False)
    raw_performances = pd.read_csv(args.raw_performances, low_memory=False)

    activities, activity_quarantine, activity_report = canonicalize_activities(
        raw_activities,
        accounts,
        contract["activity_validation"],
    )
    performances, performance_quarantine, performance_report = canonicalize_performances(
        raw_performances,
        athletes,
        disciplines,
    )
    quarantine = pd.concat(
        [activity_quarantine, performance_quarantine], ignore_index=True
    )

    atomic_parquet(activities, args.output_dir / "activities_2024.parquet")
    atomic_parquet(performances, args.output_dir / "performances_2024.parquet")
    atomic_parquet(quarantine, args.quarantine_dir / "quarantined_observations_2024.parquet")
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )
    report = {
        "dataset": "enduranceviz-2024",
        "specification_version": contract["dataset"]["specification_version"],
        "generated_at_utc": generated_at,
        "activities": activity_report,
        "performances": performance_report,
        "total_quarantine_rows": len(quarantine),
    }
    atomic_json(report, args.report)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, default=ROOT / "config" / "snapshot_2024.yaml")
    parser.add_argument(
        "--disciplines", type=Path, default=ROOT / "config" / "performance_disciplines_2024.yaml"
    )
    parser.add_argument(
        "--athletes", type=Path, default=ROOT / "data" / "reference" / "athlete_registry_2024.csv"
    )
    parser.add_argument(
        "--accounts",
        type=Path,
        default=ROOT / "data" / "reference" / "athlete_external_accounts_2024.csv",
    )
    parser.add_argument("--raw-activities", type=Path, default=ROOT / "indiv_activities_full.csv")
    parser.add_argument(
        "--raw-performances",
        type=Path,
        default=ROOT / "data" / "metadata" / "master_iaaf_database_with_strava.csv",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=ROOT / "data" / "curated" / "2024"
    )
    parser.add_argument(
        "--quarantine-dir", type=Path, default=ROOT / "data" / "quarantine" / "2024"
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=ROOT / "data" / "manifests" / "curated_observation_audit_2024.json",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        report = build(args)
    except (OSError, ValueError, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        f"Built {report['activities']['curated_rows']} activities and "
        f"{report['performances']['curated_rows']} performances"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
