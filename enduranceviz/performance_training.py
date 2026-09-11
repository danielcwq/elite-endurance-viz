"""One continuous primary-event score per athlete; no fitted association model."""

PERFORMANCE_POLICY_VERSION = 'p1-primary-event-points-exploratory-v1'

# recorded_athletes is the approved ATHLETE_RECORDED_TRAINING_SQL view.
# Aggregate BEFORE joining so repeated race results never weight an athlete twice.
PERFORMANCE_TRAINING_SQL = """
WITH scores AS (
    SELECT athlete_id, discipline,
        count(*) AS stored_performance_count,
        count(results_score) AS scored_performance_count,
        max(results_score) AS best_stored_results_score
    FROM performances_2024 GROUP BY athlete_id, discipline
)
SELECT a.*, s.best_stored_results_score,
    coalesce(s.stored_performance_count, 0) AS stored_performance_count,
    coalesce(s.scored_performance_count, 0) AS scored_performance_count
FROM recorded_athletes a LEFT JOIN scores s
    ON a.athlete_id = s.athlete_id AND a.primary_discipline = s.discipline
"""

# Metric-specific denominators; null is not an athlete's zero-training estimate.
PAIRED_METRICS_SQL = """
SELECT *, 'distance_km' AS metric,
    median_recorded_week_run_distance_meters / 1000 AS metric_value,
    distance_measured_weeks AS contributing_weeks
FROM selected
UNION ALL BY NAME
SELECT *, 'run_records' AS metric,
    median_recorded_week_run_count AS metric_value,
    recorded_run_weeks AS contributing_weeks
FROM selected
"""
