#!/usr/bin/env python3
"""Retired unsafe uploader retained only to explain the migration path."""

raise SystemExit(
    "Direct Mongo append uploads are disabled: they created duplicate activities. "
    "Build the canonical, indexed DuckDB snapshot with "
    "`.venv/bin/python scripts/pipeline_2024.py build`."
)
