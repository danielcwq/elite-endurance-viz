#!/usr/bin/env python3
"""Build 2024 coverage, weekly training, and athlete summary Parquet tables."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from enduranceviz.analytics import (
    build_athlete_summaries,
    build_coverage,
    build_weekly_training,
    load_weekly_evidence,
    reconciliation_report,
    select_weekly_evidence,
)


def atomic_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temporary, index=False)
    temporary.replace(path)


def atomic_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def git_commit() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, capture_output=True, check=False
    )
    return result.stdout.strip() or "unknown"


def build(args: argparse.Namespace) -> dict[str, Any]:
    contract = yaml.safe_load(args.contract.read_text(encoding="utf-8"))
    athletes = pd.read_csv(args.athletes, dtype={"athlete_id": str})
    accounts = pd.read_csv(
        args.accounts, dtype={"athlete_id": str, "external_account_id": str}
    )
    activities = pd.read_parquet(args.activities)
    evidence_paths = list(args.raw_weekly.glob("*.csv")) + list(args.temp_weekly.glob("metadata_*.csv"))
    evidence = load_weekly_evidence(evidence_paths, ROOT)
    selected = select_weekly_evidence(evidence)
    coverage = build_coverage(athletes, accounts, activities, selected)
    weekly = build_weekly_training(
        coverage,
        activities,
        set(contract["analytics"]["strength_activity_types"]),
        int(contract["analytics"]["rolling_window_weeks"]),
    )
    generated_at = datetime.now(timezone.utc).replace(microsecond=0)
    summaries = build_athlete_summaries(
        athletes, coverage, weekly, activities, contract, generated_at
    )
    reconciliation = reconciliation_report(activities, weekly, summaries)
    if not reconciliation["activity_count_reconciles"]:
        raise ValueError(f"Activity-count reconciliation failed: {reconciliation}")
    if reconciliation["run_distance_max_absolute_difference_meters"] > 0.001:
        raise ValueError(f"Run-distance reconciliation failed: {reconciliation}")

    atomic_parquet(coverage, args.output_dir / "data_coverage_2024.parquet")
    atomic_parquet(weekly, args.output_dir / "weekly_training_2024.parquet")
    atomic_parquet(summaries, args.output_dir / "athlete_summary_2024.parquet")
    distribution = {
        str(key): int(value)
        for key, value in summaries["coverage_status"].value_counts().sort_index().items()
    }
    report = {
        "dataset": contract["dataset"]["name"],
        "specification_version": str(contract["dataset"]["specification_version"]),
        "code_version": git_commit(),
        "generated_at_utc": generated_at.isoformat().replace("+00:00", "Z"),
        "source_evidence_rows": len(evidence),
        "selected_account_week_evidence_rows": len(selected),
        "coverage_rows": len(coverage),
        "weekly_rows": len(weekly),
        "athlete_summary_rows": len(summaries),
        "observed_zero_activity_weeks": int(
            ((coverage["observation_status"] == "observed") & ~coverage["is_active_week"]).sum()
        ),
        "missing_weeks_with_observed_activities": int(
            ((coverage["observation_status"] == "missing") & coverage["is_active_week"]).sum()
        ),
        "coverage_status_distribution": distribution,
        "default_cohort_eligible_athletes": int(summaries["default_cohort_eligible"].sum()),
        "coverage_thresholds": contract["analytics"]["coverage_score"],
        "weekly_evidence_policy": (
            "Weekly summary rows establish observation coverage only; all training metrics "
            "are recalculated from canonical deduplicated activities."
        ),
        "reconciliation": reconciliation,
    }
    atomic_json(report, args.report)
    return report


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--contract", type=Path, default=ROOT / "config" / "snapshot_2024.yaml")
    result.add_argument("--athletes", type=Path, default=ROOT / "data/reference/athlete_registry_2024.csv")
    result.add_argument("--accounts", type=Path, default=ROOT / "data/reference/athlete_external_accounts_2024.csv")
    result.add_argument("--activities", type=Path, default=ROOT / "data/curated/2024/activities_2024.parquet")
    result.add_argument("--raw-weekly", type=Path, default=ROOT / "data/raw_data")
    result.add_argument("--temp-weekly", type=Path, default=ROOT / "data/tempdata")
    result.add_argument("--output-dir", type=Path, default=ROOT / "data/curated/2024")
    result.add_argument("--report", type=Path, default=ROOT / "data/manifests/analytics_audit_2024.json")
    return result


def main() -> int:
    try:
        report = build(parser().parse_args())
    except (OSError, ValueError, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        f"Built {report['weekly_rows']} athlete-weeks and "
        f"{report['athlete_summary_rows']} athlete summaries"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
