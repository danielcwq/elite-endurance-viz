#!/usr/bin/env python3
"""Single entry point for building, validating, and serving the 2024 snapshot."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PYTHON = Path(sys.executable)


def run_script(name: str) -> None:
    result = subprocess.run([str(PYTHON), str(ROOT / "scripts" / name)], cwd=ROOT, check=False)
    if result.returncode:
        raise RuntimeError(f"{name} failed with exit code {result.returncode}")


def build() -> None:
    run_script("build_curated_observations_2024.py")
    run_script("build_analytics_2024.py")
    run_script("validate_2024.py")
    run_script("populate_serving_2024.py")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "validate", "serve"))
    args = parser.parse_args()
    try:
        if args.command == "build":
            build()
        elif args.command == "validate":
            run_script("validate_2024.py")
        else:
            run_script("populate_serving_2024.py")
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
