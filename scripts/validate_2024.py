#!/usr/bin/env python3
"""Validate all canonical 2024 tables and write machine/human-readable reports."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from enduranceviz.quality import markdown_report, validate_frames


def validate(args: argparse.Namespace) -> dict:
    contract = yaml.safe_load(args.contract.read_text(encoding="utf-8"))
    frames = {
        "athletes": pd.read_csv(args.athletes, dtype={"athlete_id": str}),
        "accounts": pd.read_csv(args.accounts, dtype={"athlete_id": str, "external_account_id": str}),
        "performances": pd.read_parquet(args.curated / "performances_2024.parquet"),
        "activities": pd.read_parquet(args.curated / "activities_2024.parquet"),
        "coverage": pd.read_parquet(args.curated / "data_coverage_2024.parquet"),
        "weekly": pd.read_parquet(args.curated / "weekly_training_2024.parquet"),
        "summaries": pd.read_parquet(args.curated / "athlete_summary_2024.parquet"),
    }
    report = validate_frames(**frames, contract=contract)
    args.json_report.parent.mkdir(parents=True, exist_ok=True)
    args.json_report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    args.markdown_report.write_text(markdown_report(report), encoding="utf-8")
    return report


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--contract", type=Path, default=ROOT / "config/snapshot_2024.yaml")
    result.add_argument("--athletes", type=Path, default=ROOT / "data/reference/athlete_registry_2024.csv")
    result.add_argument("--accounts", type=Path, default=ROOT / "data/reference/athlete_external_accounts_2024.csv")
    result.add_argument("--curated", type=Path, default=ROOT / "data/curated/2024")
    result.add_argument("--json-report", type=Path, default=ROOT / "data/manifests/data_quality_report_2024.json")
    result.add_argument("--markdown-report", type=Path, default=ROOT / "docs/example-data-quality-report-2024.md")
    return result


def main() -> int:
    try:
        report = validate(parser().parse_args())
    except (OSError, ValueError, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"Data quality: {report['status']} ({report['check_count']} checks)")
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
