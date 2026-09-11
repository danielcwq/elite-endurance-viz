"""P1 descriptive metrics for stored activity records, not complete training."""

from enduranceviz.observability import WEEK_EVIDENCE_SQL

POLICY_VERSION = 'p1-recorded-training-exploratory-v1.1'
STUDY_EVENTS = ('800m', '1500m', '3000m Steeplechase', '5000m', '10000m', 'Half Marathon')
# Preview extension requested after the six-event static exploration was frozen.
PREVIEW_EVENTS = (*STUDY_EVENTS, 'Marathon')

RECORDED_WEEK_METRICS_SQL = f"""
WITH evidence AS ({WEEK_EVIDENCE_SQL}), measurements AS (
    SELECT athlete_id, week_start_utc,
        count(DISTINCT (start_at_utc AT TIME ZONE 'UTC')::DATE)
            FILTER (WHERE activity_category = 'Run') AS recorded_run_days,
        count(*) FILTER (WHERE activity_category = 'Run' AND distance_meters IS NULL)
            AS runs_missing_distance,
        count(*) FILTER (WHERE activity_category = 'Run' AND distance_meters IS NOT NULL
            AND (NOT isfinite(distance_meters) OR distance_meters < 0)) AS runs_invalid_distance,
        sum(distance_meters) FILTER (WHERE activity_category = 'Run'
            AND isfinite(distance_meters) AND distance_meters >= 0) AS known_run_distance_meters
    FROM activities_2024 GROUP BY athlete_id, week_start_utc
)
SELECT e.athlete_id, e.week_start_utc, e.is_partial_window, e.evidence_state,
    e.collection_error_code, e.posted_activities, e.posted_runs,
    coalesce(m.recorded_run_days, 0) AS recorded_run_days,
    coalesce(m.runs_missing_distance, 0) AS runs_missing_distance,
    coalesce(m.runs_invalid_distance, 0) AS runs_invalid_distance,
    m.known_run_distance_meters,
    CASE WHEN e.posted_runs > 0 AND m.runs_missing_distance = 0 AND m.runs_invalid_distance = 0
        THEN m.known_run_distance_meters ELSE NULL END AS recorded_week_run_distance_meters
FROM evidence e LEFT JOIN measurements m USING(athlete_id, week_start_utc)
"""

ATHLETE_RECORDED_TRAINING_SQL = f"""
WITH weeks AS ({RECORDED_WEEK_METRICS_SQL}), summaries AS (
    SELECT athlete_id,
        sum(posted_runs) AS recorded_runs_2024,
        sum(posted_activities) AS recorded_activities_2024,
        count(*) FILTER (WHERE NOT is_partial_window AND posted_runs > 0) AS recorded_run_weeks,
        count(*) FILTER (WHERE NOT is_partial_window AND recorded_week_run_distance_meters IS NOT NULL)
            AS distance_measured_weeks,
        count(*) FILTER (WHERE NOT is_partial_window AND posted_runs > 0
            AND recorded_week_run_distance_meters IS NULL) AS distance_unavailable_run_weeks,
        count(*) FILTER (WHERE NOT is_partial_window AND posted_runs > 0
            AND evidence_state = 'source_warning') AS source_warning_run_weeks,
        sum(posted_runs) FILTER (WHERE NOT is_partial_window) AS recorded_runs_full_weeks,
        median(posted_runs) FILTER (WHERE NOT is_partial_window AND posted_runs > 0)
            AS median_recorded_week_run_count,
        median(recorded_run_days) FILTER (WHERE NOT is_partial_window AND posted_runs > 0)
            AS median_recorded_week_run_days,
        median(recorded_week_run_distance_meters) FILTER (WHERE NOT is_partial_window)
            AS median_recorded_week_run_distance_meters
    FROM weeks GROUP BY athlete_id
)
SELECT d.athlete_id::VARCHAR AS athlete_id, d.primary_discipline, d.gender,
    s.* EXCLUDE(athlete_id)
FROM athlete_directory_2024 d JOIN summaries s USING(athlete_id)
"""
