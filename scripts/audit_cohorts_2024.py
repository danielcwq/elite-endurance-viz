#!/usr/bin/env python3
"""Write aggregate P1 planning evidence without changing the P0 database."""

import argparse
import hashlib
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]


def audit(database: Path) -> str:
    sections = [
        ("Selected comparison: 800m versus 1500m — coverage readiness", """
            WITH per_athlete AS (
                SELECT d.athlete_id, d.primary_discipline, d.gender,
                       count(*) FILTER (WHERE w.is_complete_enough_week) AS usable_weeks,
                       count(*) FILTER (WHERE w.coverage_status = 'observed_with_warning') AS warning_weeks,
                       count(*) FILTER (WHERE w.rolling_4w_average_run_distance_meters IS NOT NULL) AS rolling_windows
                FROM athlete_directory_2024 d
                JOIN weekly_training_2024 w USING (athlete_id)
                WHERE d.default_cohort_eligible AND d.primary_discipline IN ('800m', '1500m')
                GROUP BY d.athlete_id, d.primary_discipline, d.gender
            )
            SELECT primary_discipline AS event, gender AS recorded_sex,
                   count(*) AS athletes, min(usable_weeks) AS minimum_usable_weeks,
                   count(*) FILTER (WHERE usable_weeks >= 26) AS candidate_26_week_floor,
                   count(*) FILTER (WHERE usable_weeks >= 39) AS candidate_39_week_floor,
                   median(usable_weeks) AS median_usable_weeks,
                   max(usable_weeks) AS maximum_usable_weeks,
                   sum(warning_weeks) AS warning_athlete_weeks,
                   min(rolling_windows) AS minimum_rolling_windows
            FROM per_athlete GROUP BY 1,2 ORDER BY 1,2
        """),
        ("Selected comparison: run measurement availability in usable weeks", """
            SELECT d.primary_discipline AS event, d.gender AS recorded_sex,
                   count(*) AS run_activities,
                   count(*) FILTER (WHERE a.distance_meters IS NULL) AS missing_distance,
                   count(*) FILTER (WHERE a.distance_meters = 0) AS zero_distance,
                   count(*) FILTER (WHERE coalesce(a.moving_seconds, a.elapsed_seconds) IS NULL) AS missing_duration,
                   count(*) FILTER (WHERE a.quality_status = 'warning') AS warning_activities
            FROM athlete_directory_2024 d
            JOIN activities_2024 a USING (athlete_id)
            JOIN weekly_training_2024 w ON w.athlete_id = a.athlete_id AND w.week_start_utc = a.week_start_utc
            WHERE d.default_cohort_eligible AND d.primary_discipline IN ('800m', '1500m')
                  AND w.is_complete_enough_week AND a.activity_category = 'Run'
            GROUP BY 1,2 ORDER BY 1,2
        """),
        ("Selected comparison: athletes with performances in both events", """
            WITH both_events AS (
                SELECT athlete_id FROM performances_2024
                WHERE discipline IN ('800m', '1500m') GROUP BY athlete_id
                HAVING count(DISTINCT discipline) = 2
            )
            SELECT d.primary_discipline AS assigned_event, d.gender AS recorded_sex,
                   count(*) AS eligible_athletes_with_both_events
            FROM athlete_directory_2024 d JOIN both_events USING (athlete_id)
            WHERE d.default_cohort_eligible AND d.primary_discipline IN ('800m', '1500m')
            GROUP BY 1,2 ORDER BY 1,2
        """),
        ("Coverage inventory", """
            SELECT coalesce(coverage_status, 'missing summary') AS coverage,
                   count(*) AS athletes,
                   count(*) FILTER (WHERE default_cohort_eligible) AS eligible
            FROM athlete_directory_2024 GROUP BY 1 ORDER BY 1
        """),
        ("Primary event and recorded sex", """
            SELECT coalesce(primary_discipline, 'No primary event') AS event,
                   gender AS recorded_sex, count(*) AS registry,
                   count(*) FILTER (WHERE default_cohort_eligible) AS eligible,
                   count(*) FILTER (WHERE coverage_status = 'high') AS high,
                   count(*) FILTER (WHERE coverage_status = 'moderate') AS moderate,
                   count(*) FILTER (WHERE NOT coalesce(default_cohort_eligible, false)) AS excluded
            FROM athlete_directory_2024 GROUP BY 1,2 ORDER BY 1,2
        """),
        ("Candidate score bands — planning only", """
            WITH primary_scores AS (
                SELECT athlete_id, max(results_score) AS score
                FROM performances_2024 WHERE is_primary_discipline GROUP BY athlete_id
            ), banded AS (
                SELECT d.*, CASE WHEN score IS NULL THEN 'Missing score'
                    WHEN score < 1100 THEN 'Below 1100'
                    WHEN score < 1150 THEN '1100–1149'
                    WHEN score < 1200 THEN '1150–1199'
                    WHEN score < 1250 THEN '1200–1249' ELSE '1250+' END AS score_band
                FROM athlete_directory_2024 d LEFT JOIN primary_scores USING (athlete_id)
            )
            SELECT coalesce(primary_discipline, 'No primary event') AS event,
                   gender AS recorded_sex, score_band, count(*) AS eligible
            FROM banded WHERE default_cohort_eligible GROUP BY 1,2,3 ORDER BY 1,2,3
        """),
    ]
    with duckdb.connect(str(database), read_only=True) as connection:
        total, unique, eligible, assigned = connection.execute("""
            SELECT count(*), count(DISTINCT athlete_id),
                   count(*) FILTER (WHERE default_cohort_eligible),
                   count(*) FILTER (WHERE default_cohort_eligible AND primary_discipline IS NOT NULL)
            FROM athlete_directory_2024
        """).fetchone()
        if total != unique:
            raise ValueError("Directory contains duplicate athlete IDs; cohort counts would be inflated")
        conflicts = connection.execute("""
            SELECT count(*) FROM (
                SELECT athlete_id FROM performances_2024 WHERE is_primary_discipline
                GROUP BY athlete_id HAVING count(DISTINCT discipline) > 1
            )
        """).fetchone()[0]
        if conflicts:
            raise ValueError("Multiple primary disciplines per athlete")
        report = [
            "# P1 initial cohort audit", "",
            f"Database SHA-256: `{hashlib.sha256(database.read_bytes()).hexdigest()}`", "",
            "Reproduce: `.venv/bin/python scripts/audit_cohorts_2024.py`", "",
            "This is planning evidence, not a finalized analysis protocol. No training outcomes are compared here.", "",
            "Daniel selected 800m versus 1500m as the first comparison. Readiness tables use the existing "
            "P0 eligibility and usable-week flags; these are reference counts pending approval of the analysis protocol. "
            "A usable week has collection evidence with no recorded contradiction and is a full calendar week. "
            "This does not guarantee that all training was publicly posted. Missing and warning weeks are not imputed. "
            "Rolling windows require four consecutive usable weeks under P0 rules.", "",
            f"Registry: **{total:,}** unique athletes. P0 coverage-eligible: **{eligible:,}**. "
            f"Eligible with an assigned primary event: **{assigned:,}**. "
            f"Excluded by coverage: **{total - eligible:,}**. "
            f"Coverage-eligible but missing an event: **{eligible - assigned:,}**.", "",
            "Counts use one directory row per athlete and the existing P0 primary-event assignment. "
            "Eligible means the stored P0 high/moderate coverage flag; it does not establish representativeness "
            "or complete training capture. Recorded sex comes from the existing source classification.", "",
            "Score bands below are provisional 50-point bins for inspecting sample sizes. They are not approved "
            "performance tiers. Scores use the maximum results score within the assigned primary discipline. "
            "The source performances have already been selected at 1,100 points or above; this dataset alone "
            "cannot estimate what was excluded below that threshold.", "",
        ]
        for title, query in sections:
            result = connection.execute(query)
            headers = [column[0].replace('_', ' ') for column in result.description]
            report += [f"## {title}", "", "| " + " | ".join(headers) + " |",
                       "| " + " | ".join('---' for _ in headers) + " |"]
            report += ["| " + " | ".join(str(value) for value in row) + " |" for row in result.fetchall()]
            report.append("")
    return "\n".join(report)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, default=ROOT / 'deploy/enduranceviz_2024.duckdb')
    parser.add_argument('--output', type=Path, default=ROOT / 'docs/p1-cohort-audit-2024.md')
    args = parser.parse_args()
    args.output.write_text(audit(args.database), encoding='utf-8')
    print(f"Wrote aggregate cohort audit to {args.output}")
