"""Read-only, projection-first access to the canonical 2024 serving database."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import duckdb


ROOT = Path(__file__).resolve().parents[1]
LOCAL_DATABASE = ROOT / "data/derived/2024/enduranceviz_2024.duckdb"
PACKAGED_DATABASE = ROOT / "deploy/enduranceviz_2024.duckdb"


def default_database() -> Path:
    """Prefer a local rebuild, then the immutable deployment artifact."""
    return LOCAL_DATABASE if LOCAL_DATABASE.is_file() else PACKAGED_DATABASE


class ServingRepository:
    def __init__(self, database: Path | None = None) -> None:
        configured = os.getenv("ENDURANCEVIZ_DB_PATH")
        self.database = Path(configured).expanduser().resolve() if configured else (database or default_database())
        if not self.database.is_file():
            raise FileNotFoundError(
                f"Canonical serving database is missing at {self.database}. "
                "Run `.venv/bin/python scripts/pipeline_2024.py build` locally or "
                "package it with `.venv/bin/python scripts/package_serving_artifact.py`."
            )

    def _query(self, sql: str, parameters: list[Any] | None = None) -> list[dict[str, Any]]:
        with duckdb.connect(str(self.database), read_only=True) as connection:
            cursor = connection.execute(sql, parameters or [])
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    @lru_cache(maxsize=1)
    def snapshot_stats(self) -> dict[str, Any]:
        return self._query(
            """
            SELECT
                (SELECT count(*) FROM athletes) AS athlete_count,
                (SELECT count(DISTINCT nationality_code) FROM athletes
                 WHERE nationality_code IS NOT NULL) AS country_count,
                (SELECT count(*) FROM activities_2024) AS activity_count,
                (SELECT count(*) FROM athlete_external_accounts WHERE provider = 'strava') AS strava_account_count,
                specification_version AS dataset_version,
                completed_at_utc AS build_time,
                source_commit
            FROM dataset_builds
            WHERE status = 'succeeded'
            ORDER BY completed_at_utc DESC
            LIMIT 1
            """
        )[0]

    def search_athletes(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        pattern = f"%{query.strip()}%"
        return self._query(
            """
            SELECT athlete_id::VARCHAR AS athlete_id,
                   coalesce(display_name, official_name) AS display_name,
                   official_name, nationality_code, primary_discipline,
                   coverage_status
            FROM athlete_directory_2024
            WHERE official_name ILIKE ? OR coalesce(display_name, '') ILIKE ?
            ORDER BY
                CASE WHEN coalesce(display_name, official_name) ILIKE ? THEN 0 ELSE 1 END,
                coalesce(display_name, official_name)
            LIMIT ?
            """,
            [pattern, pattern, f"{query.strip()}%", min(max(limit, 1), 25)],
        )

    @lru_cache(maxsize=1)
    def map_athletes(self) -> tuple[dict[str, Any], ...]:
        rows = self._query(
            """
            SELECT athlete_id::VARCHAR AS athlete_id,
                   coalesce(display_name, official_name) AS display_name,
                   nationality_code, primary_discipline, coverage_status
            FROM athlete_directory_2024
            WHERE nationality_code IS NOT NULL
            ORDER BY nationality_code, display_name
            """
        )
        return tuple(rows)

    @lru_cache(maxsize=4096)
    def athlete(self, athlete_id: str) -> dict[str, Any] | None:
        rows = self._query(
            """
            SELECT athlete_id::VARCHAR AS athlete_id, official_name, display_name,
                   nationality_code, gender, primary_discipline, coverage_status,
                   coverage_score, default_cohort_eligible, total_activity_count,
                   total_run_distance_meters, total_run_duration_seconds,
                   average_run_distance_per_observed_week_meters,
                   average_run_distance_per_calendar_week_meters,
                   weighted_run_pace_seconds_per_kilometer, observed_weeks,
                   dataset_version, computed_at_utc
            FROM athlete_directory_2024
            WHERE athlete_id = try_cast(? AS UUID)
            """,
            [athlete_id],
        )
        return rows[0] if rows else None

    @lru_cache(maxsize=4096)
    def season_bests(self, athlete_id: str) -> tuple[dict[str, Any], ...]:
        return tuple(
            self._query(
                """
                SELECT discipline, mark_text, performance_date, location, results_score
                FROM performances_2024
                WHERE athlete_id = try_cast(? AS UUID) AND is_season_best
                ORDER BY discipline, performance_date, mark_text
                """,
                [athlete_id],
            )
        )

    @lru_cache(maxsize=4096)
    def external_accounts(self, athlete_id: str) -> tuple[dict[str, Any], ...]:
        return tuple(
            self._query(
                """
                SELECT provider, external_account_id, provider_display_name
                FROM athlete_external_accounts
                WHERE athlete_id = try_cast(? AS UUID)
                ORDER BY provider, external_account_id
                """,
                [athlete_id],
            )
        )

    def activities(self, athlete_id: str, page: int = 1, page_size: int = 30) -> dict[str, Any]:
        page_size = min(max(int(page_size), 1), 50)
        page = max(int(page), 1)
        offset = (page - 1) * page_size
        rows = self._query(
            """
            SELECT activity_id, activity_name, description, provider_activity_type,
                   activity_category, start_at_utc, distance_meters,
                   elapsed_seconds, moving_seconds, pace_seconds_per_kilometer,
                   location, quality_status, quality_flags
            FROM activities_2024
            WHERE athlete_id = try_cast(? AS UUID)
            ORDER BY start_at_utc DESC, activity_id DESC
            LIMIT ? OFFSET ?
            """,
            [athlete_id, page_size + 1, offset],
        )
        has_next = len(rows) > page_size
        return {
            "rows": rows[:page_size],
            "page": page,
            "page_size": page_size,
            "has_previous": page > 1,
            "has_next": has_next,
        }
