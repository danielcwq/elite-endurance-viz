"""P1 evidence views: collection state is separate from observed posting.

These views reinterpret the frozen P0 records without altering their provenance.
They do not establish completeness of public capture or of actual training.
"""

WEEK_EVIDENCE_SQL = """
WITH activity_counts AS (
    SELECT athlete_id, week_start_utc,
           count(*) AS posted_activities,
           count(*) FILTER (WHERE activity_category = 'Run') AS posted_runs,
           count(*) FILTER (WHERE activity_category = 'Run' AND distance_meters IS NULL)
               AS runs_missing_distance,
           sum(distance_meters) FILTER (WHERE activity_category = 'Run') AS known_run_distance_meters
    FROM activities_2024 GROUP BY athlete_id, week_start_utc
)
SELECT c.athlete_id, c.week_start_utc, c.is_partial_window,
       c.coverage_status AS legacy_coverage_status,
       c.collection_error_code, c.evidence_source,
       CASE
           WHEN coalesce(c.collection_error_code, '') <> ''
                OR c.coverage_status = 'observed_with_warning' THEN 'source_warning'
           WHEN c.observation_status = 'observed' AND coalesce(a.posted_activities, 0) = 0
                THEN 'ambiguous_empty_record'
           WHEN c.observation_status = 'observed' THEN 'activity_and_weekly_record'
           WHEN coalesce(a.posted_activities, 0) > 0 THEN 'activity_without_weekly_record'
           ELSE 'no_collection_evidence'
       END AS evidence_state,
       coalesce(a.posted_activities, 0) AS posted_activities,
       coalesce(a.posted_runs, 0) AS posted_runs,
       coalesce(a.runs_missing_distance, 0) AS runs_missing_distance,
       a.known_run_distance_meters
FROM data_coverage_2024 c
LEFT JOIN activity_counts a USING (athlete_id, week_start_utc)
"""


ATHLETE_POSTING_SQL = f"""
WITH weeks AS ({WEEK_EVIDENCE_SQL}),
full_weeks AS (
    SELECT * FROM weeks WHERE NOT is_partial_window
), empty_runs AS (
    SELECT athlete_id, week_start_utc,
           row_number() OVER (PARTITION BY athlete_id ORDER BY week_start_utc)
           - row_number() OVER (PARTITION BY athlete_id, posted_runs > 0 ORDER BY week_start_utc)
               AS gap_group,
           posted_runs
    FROM full_weeks
), gaps AS (
    SELECT athlete_id, gap_group, count(*) AS gap_length
    FROM empty_runs WHERE posted_runs = 0 GROUP BY athlete_id, gap_group
), longest_gaps AS (
    SELECT athlete_id, max(gap_length) AS longest_no_recorded_run_gap FROM gaps GROUP BY athlete_id
), annual AS (
    SELECT athlete_id, sum(posted_runs) AS recorded_runs_2024,
           sum(posted_activities) AS recorded_activities_2024
    FROM weeks GROUP BY athlete_id
), summary AS (
    SELECT athlete_id, count(*) AS full_calendar_weeks,
           count(*) FILTER (WHERE evidence_state = 'ambiguous_empty_record') AS ambiguous_empty_weeks,
           count(*) FILTER (WHERE evidence_state = 'source_warning') AS warning_weeks,
           count(*) FILTER (WHERE evidence_state = 'activity_and_weekly_record') AS activity_and_record_weeks,
           count(*) FILTER (WHERE evidence_state = 'activity_without_weekly_record') AS activity_only_weeks,
           count(*) FILTER (WHERE evidence_state = 'no_collection_evidence') AS no_evidence_weeks,
           count(*) FILTER (WHERE posted_runs > 0) AS weeks_with_runs,
           count(*) FILTER (WHERE posted_activities > 0) AS weeks_with_activities,
           count(*) FILTER (WHERE runs_missing_distance > 0) AS weeks_with_missing_run_distance,
           min(week_start_utc) FILTER (WHERE posted_runs > 0) AS first_week_with_runs,
           max(week_start_utc) FILTER (WHERE posted_runs > 0) AS last_week_with_runs
    FROM full_weeks GROUP BY athlete_id
)
SELECT d.athlete_id::VARCHAR AS athlete_id, coalesce(d.display_name, d.official_name) AS display_name,
       d.primary_discipline, d.gender, d.default_cohort_eligible AS legacy_p0_eligible,
       s.* EXCLUDE (athlete_id),
       coalesce(g.longest_no_recorded_run_gap, 0) AS longest_no_recorded_run_gap,
       a.recorded_runs_2024, a.recorded_activities_2024
FROM athlete_directory_2024 d
JOIN summary s USING (athlete_id)
JOIN annual a USING (athlete_id)
LEFT JOIN longest_gaps g USING (athlete_id)
"""
