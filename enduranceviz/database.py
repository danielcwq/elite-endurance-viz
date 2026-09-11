"""Serving indexes and directory view shared by canonical and synthetic builds."""

SERVING_OBJECTS_SQL = """
CREATE INDEX idx_activities_athlete_start ON activities_2024 (athlete_id, start_at_utc);
CREATE INDEX idx_performances_athlete_date ON performances_2024 (athlete_id, performance_date);
CREATE INDEX idx_external_accounts_athlete ON athlete_external_accounts (athlete_id);

CREATE VIEW athlete_directory_2024 AS
WITH primary_disciplines AS (
    SELECT athlete_id, min(discipline) AS primary_discipline
    FROM performances_2024 WHERE is_primary_discipline GROUP BY athlete_id
)
SELECT a.*, p.primary_discipline, s.coverage_status, s.coverage_score,
       s.default_cohort_eligible, s.total_activity_count,
       s.total_run_distance_meters, s.total_run_duration_seconds,
       s.average_run_distance_per_observed_week_meters,
       s.average_run_distance_per_calendar_week_meters,
       s.weighted_run_pace_seconds_per_kilometer,
       s.observed_weeks, s.dataset_version, s.computed_at_utc
FROM athletes a
LEFT JOIN primary_disciplines p USING (athlete_id)
LEFT JOIN athlete_summary_2024 s USING (athlete_id);
"""
