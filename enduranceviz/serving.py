"""Read-only, projection-first access to the canonical 2024 serving database."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import duckdb

from enduranceviz.activity_filters import ActivityFilters
from enduranceviz.recorded_training import RECORDED_WEEK_METRICS_SQL, ATHLETE_RECORDED_TRAINING_SQL, STUDY_EVENTS
from enduranceviz.performance_training import PERFORMANCE_TRAINING_SQL


ROOT = Path(__file__).resolve().parents[1]
LOCAL_DATABASE = ROOT / "data/derived/2024/enduranceviz_2024.duckdb"
PACKAGED_DATABASE = ROOT / "deploy/enduranceviz_2024.duckdb"


def default_database() -> Path:
    """Prefer a local rebuild, then the immutable deployment artifact."""
    return LOCAL_DATABASE if LOCAL_DATABASE.is_file() else PACKAGED_DATABASE


class ServingRepository:
    def __init__(self, database: Path | None = None) -> None:
        configured = os.getenv("ENDURANCEVIZ_DB_PATH")
        # An explicit database (including a test fixture) wins over process-wide
        # configuration. The application still uses the environment by default.
        self.database = Path(database or configured or default_database()).expanduser().resolve()
        if not self.database.is_file():
            raise FileNotFoundError(
                f"Canonical serving database is missing at {self.database}. "
                "Run `.venv/bin/python scripts/pipeline_2024.py build` locally or "
                "package it with `.venv/bin/python scripts/package_serving_artifact.py`."
            )

    def _query(self, sql: str, parameters: list[Any] | None = None) -> list[dict[str, Any]]:
        with duckdb.connect(str(self.database), read_only=True) as connection:
            return self._fetch(connection, sql, parameters)

    @staticmethod
    def _fetch(connection, sql: str, parameters: list[Any] | None = None) -> list[dict[str, Any]]:
        cursor = connection.execute(sql, parameters or [])
        columns = [description[0] for description in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def health_metadata(self) -> dict[str, Any] | None:
        """Fresh read-only readiness probe, deliberately outside all caches."""
        rows = self._query('''
            SELECT specification_version AS dataset_version, completed_at_utc AS build_time,
                   status,
                   EXISTS(SELECT 1 FROM athlete_directory_2024 LIMIT 1) AS has_athletes,
                   EXISTS(SELECT 1 FROM activities_2024 LIMIT 1) AS has_activities,
                   EXISTS(SELECT 1 FROM data_coverage_2024 LIMIT 1) AS has_coverage
            FROM dataset_builds ORDER BY started_at_utc DESC, completed_at_utc DESC NULLS LAST
            LIMIT 1
        ''')
        if not rows:
            return None
        row = rows[0]
        if (row['status'] != 'succeeded' or not row['build_time'] or not row['dataset_version']
                or not all(row[key] for key in ('has_athletes', 'has_activities', 'has_coverage'))):
            return None
        return {'dataset_version': row['dataset_version'], 'build_time': row['build_time']}

    @lru_cache(maxsize=1)
    def comparison_athletes(self) -> tuple[dict[str, Any], ...]:
        """Approved exploratory summaries; no P0 eligibility or week cutoff."""
        return tuple(self._query(f'''
            WITH recorded_athletes AS ({ATHLETE_RECORDED_TRAINING_SQL}),
                 scored AS ({PERFORMANCE_TRAINING_SQL})
            SELECT s.*, coalesce(d.display_name, d.official_name) AS name
            FROM scored s JOIN athlete_directory_2024 d USING(athlete_id)
            WHERE s.primary_discipline IN ({','.join('?' for _ in STUDY_EVENTS)})
            ORDER BY s.primary_discipline, s.gender, name, s.athlete_id
        ''', list(STUDY_EVENTS)))

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
    def map_countries(self) -> tuple[dict[str, Any], ...]:
        """Return compact country-level counts for the map's initial load."""
        return tuple(
            self._query(
                """
                SELECT nationality_code,
                       count(*) AS athlete_count,
                       count(*) FILTER (WHERE total_activity_count > 0) AS observed_athlete_count,
                       count(*) FILTER (WHERE coverage_status IN ('high', 'moderate'))
                           AS coverage_qualified_count
                FROM athlete_directory_2024
                WHERE nationality_code IS NOT NULL
                GROUP BY nationality_code
                ORDER BY athlete_count DESC, nationality_code
                """
            )
        )

    @lru_cache(maxsize=256)
    def map_country_athletes(self, country_code: str) -> tuple[dict[str, Any], ...]:
        """Return the roster for one selected nationality code."""
        rows = self._query(
            """
            SELECT athlete_id::VARCHAR AS athlete_id,
                   coalesce(display_name, official_name) AS display_name,
                   nationality_code, primary_discipline, coverage_status
            FROM athlete_directory_2024
            WHERE nationality_code = ?
            ORDER BY display_name
            """,
            [country_code.strip().upper()],
        )
        return tuple(rows)

    @lru_cache(maxsize=1)
    def map_athletes(self) -> tuple[dict[str, Any], ...]:
        """Retain the original full projection for API compatibility."""
        rows: list[dict[str, Any]] = []
        for country in self.map_countries():
            rows.extend(self.map_country_athletes(country["nationality_code"]))
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

    def activities(self, athlete_id: str, page: int = 1, page_size: int = 30,
                   filters: ActivityFilters | None = None) -> dict[str, Any]:
        page_size = min(max(int(page_size), 1), 50)
        page = max(int(page), 1)
        predicates = ['athlete_id = try_cast(? AS UUID)']
        parameters: list[Any] = [athlete_id]
        if filters is not None:
            predicates.extend(['start_at_utc >= ?', 'start_at_utc < ?'])
            parameters.extend(filters.utc_bounds())
            if filters.category != 'All':
                predicates.append('activity_category = ?')
                parameters.append(filters.category)
        where = ' AND '.join(predicates)
        # Both reads belong to this request. Share the connection without
        # retaining a process-global handle or broadening the row projection.
        with duckdb.connect(str(self.database), read_only=True) as connection:
            total = self._fetch(connection, f'SELECT count(*) AS n FROM activities_2024 WHERE {where}', parameters)[0]['n']
            total_pages = max(1, (total + page_size - 1) // page_size)
            page = min(page, total_pages)
            offset = (page - 1) * page_size
            rows = self._fetch(connection,
                f"""
                SELECT activity_id, activity_name, description, provider_activity_type,
                       activity_category, start_at_utc, distance_meters,
                       elapsed_seconds, moving_seconds, pace_seconds_per_kilometer,
                       location, quality_status, quality_flags
                FROM activities_2024
                WHERE {where}
                ORDER BY start_at_utc DESC, activity_id DESC
                LIMIT ? OFFSET ?
                """,
                [*parameters, page_size + 1, offset],
            )
        has_next = len(rows) > page_size
        return {
            "rows": rows[:page_size],
            "page": page,
            "page_size": page_size,
            "has_previous": page > 1,
            "has_next": has_next,
            "total": total,
            "total_pages": total_pages,
            "first": offset + 1 if total else 0,
            "last": min(offset + page_size, total),
        }

    @lru_cache(maxsize=128)
    def recorded_weeks(self, athlete_id: str) -> tuple[dict[str, Any], ...]:
        """53 bounded rows from actual records, scoped before metric aggregation."""
        return tuple(self._query(
            f"""WITH activities_2024 AS (
                SELECT athlete_id, week_start_utc, activity_category, distance_meters, start_at_utc
                FROM main.activities_2024 WHERE athlete_id=try_cast(? AS UUID)
            ), data_coverage_2024 AS (
                SELECT athlete_id, week_start_utc, is_partial_window, coverage_status,
                    collection_error_code, evidence_source, observation_status
                FROM main.data_coverage_2024 WHERE athlete_id=try_cast(? AS UUID)
            ) SELECT * FROM ({RECORDED_WEEK_METRICS_SQL}) ORDER BY week_start_utc""",
            [athlete_id, athlete_id],
        ))
