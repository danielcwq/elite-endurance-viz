#!/usr/bin/env python3
"""Retired drop-and-rebuild updater retained only as a migration guard."""

raise SystemExit(
    "Mongo drop-and-rebuild refreshes are disabled for the fixed 2024 snapshot. "
    "Use `.venv/bin/python scripts/pipeline_2024.py build`; the serving database "
    "is populated through an atomic file replacement."
)
