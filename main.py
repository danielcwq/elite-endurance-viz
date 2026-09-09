"""FastHTML application for the fixed EnduranceViz 2024 snapshot."""

from __future__ import annotations

import os
from datetime import datetime
from functools import lru_cache

from fasthtml.common import *
from starlette.responses import JSONResponse

from enduranceviz.serving import ServingRepository


repository = ServingRepository()

app, rt = fast_app(
    # FastHTML otherwise writes a generated key to .sesskey during import,
    # which is incompatible with Vercel's read-only function filesystem.
    secret_key=os.getenv("ENDURANCEVIZ_SESSION_SECRET", "enduranceviz-2024-read-only-snapshot"),
    hdrs=(
        Link(rel="stylesheet", href="https://cdn.jsdelivr.net/npm/@picocss/pico@1/css/pico.min.css"),
        Style(
            """
            :root { --max-width: 1120px; }
            body { padding-bottom: 4rem; }
            .snapshot-banner { background: #eef6ff; border: 1px solid #b9d7f5; padding: .75rem 1rem; border-radius: .5rem; }
            .search-wrap { position: relative; max-width: 760px; }
            .search-results { position: absolute; inset: 100% 0 auto; z-index: 10; background: white; border: 1px solid #ddd; border-radius: .4rem; box-shadow: 0 8px 20px #0002; }
            .search-results a { display: block; padding: .7rem 1rem; text-decoration: none; border-bottom: 1px solid #eee; }
            .search-results a:last-child { border-bottom: 0; }
            .metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1rem; }
            .metric { padding: 1rem; border: 1px solid #ddd; border-radius: .5rem; }
            .metric strong { display: block; font-size: 1.35rem; }
            .muted { color: #667085; }
            .season-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: .75rem; }
            .season-card { border: 1px solid #ddd; border-radius: .5rem; padding: .8rem; }
            .table-wrap { overflow-x: auto; }
            .pagination { display: flex; gap: 1rem; align-items: center; justify-content: center; }
            @media (prefers-color-scheme: dark) {
              .snapshot-banner { background: #102b43; border-color: #28628f; }
              .search-results { background: #18232d; border-color: #394956; }
              .search-results a { border-color: #394956; }
            }
            """
        ),
    ),
)

# Vercel's Python entrypoint scanner only recognizes simple top-level names.
# Keep FastHTML's ``app, rt`` construction for local development while exposing
# an explicit ASGI application for the deployment runtime.
application = app


def format_timestamp(value) -> str:
    if value is None:
        return "Unknown"
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value.strftime("%Y-%m-%d %H:%M UTC")


def format_metric(value, divisor: float = 1, suffix: str = "", decimals: int = 1) -> str:
    if value is None:
        return "Missing"
    return f"{float(value) / divisor:,.{decimals}f}{suffix}"


def snapshot_banner(stats: dict) -> Div:
    return Div(
        Strong("2024 Snapshot"),
        " · ",
        Span(f"dataset v{stats['dataset_version']}"),
        " · ",
        Span(f"built {format_timestamp(stats['build_time'])}"),
        Br(),
        Small("Publicly observed Strava activity, not a complete or current training history."),
        cls="snapshot-banner",
    )


@rt("/api/athletes/search")
def athlete_search(q: str = ""):
    if len(q.strip()) < 2:
        return JSONResponse([])
    return JSONResponse(repository.search_athletes(q))


@rt("/api/athletes/map")
def athlete_map():
    # Projection-only and loaded on demand; it is never embedded in homepage HTML.
    return JSONResponse(list(repository.map_athletes()))


@rt("/api/snapshot")
def snapshot_metadata():
    stats = repository.snapshot_stats().copy()
    for key, value in list(stats.items()):
        if isinstance(value, datetime):
            stats[key] = value.isoformat()
    return JSONResponse(stats)


@lru_cache(maxsize=1)
def homepage():
    stats = repository.snapshot_stats()
    return Titled(
        "EnduranceViz — 2024 Snapshot",
        Main(
            snapshot_banner(stats),
            H1("Elite endurance training, observed in 2024"),
            P(
                "Search the canonical athlete registry. Results and training summaries are "
                "linked by stable internal IDs rather than athlete-name joins.",
                cls="muted",
            ),
            Div(
                Input(type="search", id="athlete-search", placeholder="Search an athlete…", autocomplete="off"),
                Div(id="search-results", cls="search-results", hidden=True),
                cls="search-wrap",
            ),
            Script(
                """
                const input = document.getElementById('athlete-search');
                const output = document.getElementById('search-results');
                let timer;
                input.addEventListener('input', () => {
                  clearTimeout(timer);
                  timer = setTimeout(async () => {
                    const query = input.value.trim();
                    if (query.length < 2) { output.hidden = true; output.innerHTML = ''; return; }
                    const rows = await fetch('/api/athletes/search?q=' + encodeURIComponent(query)).then(r => r.json());
                    output.innerHTML = rows.length ? rows.map(row =>
                      `<a href="/athlete/${row.athlete_id}"><strong>${row.display_name}</strong><br>` +
                      `<small>${row.nationality_code || '—'} · ${row.primary_discipline || 'No 2024 discipline'} · ${row.coverage_status || 'unknown'} coverage</small></a>`
                    ).join('') : '<a>No athletes found</a>';
                    output.hidden = false;
                  }, 180);
                });
                document.addEventListener('click', event => {
                  if (!output.contains(event.target) && event.target !== input) output.hidden = true;
                });
                """
            ),
            H2("Snapshot inventory"),
            Div(
                Div(Strong(f"{stats['athlete_count']:,}"), Span("athletes"), cls="metric"),
                Div(Strong(f"{stats['strava_account_count']:,}"), Span("resolved Strava accounts"), cls="metric"),
                Div(Strong(f"{stats['activity_count']:,}"), Span("unique 2024 activities"), cls="metric"),
                Div(Strong(f"{stats['country_count']:,}"), Span("nationality codes"), cls="metric"),
                cls="metric-grid",
            ),
            H2("What the numbers mean"),
            P(
                "Observed-zero weeks are distinct from missing weeks. Weekly and athlete totals are "
                "recalculated from 139,887 deduplicated activities; old CSV summary totals are not served."
            ),
            P(A("Dataset methods and limitations", href="https://github.com/danielcwq/elite-endurance-viz/blob/p0-2024-data-foundation/docs/data-specification-2024-v1.md")),
            cls="container",
        ),
    )


@rt("/")
def get():
    return homepage()


def activity_row(row: dict) -> Tr:
    duration = row["moving_seconds"] if row["moving_seconds"] is not None else row["elapsed_seconds"]
    return Tr(
        Td(format_timestamp(row["start_at_utc"]).replace(" 00:00 UTC", "")),
        Td(
            A(
                row["activity_name"] or "Untitled activity",
                href=f"https://strava.com/activities/{row['activity_id']}",
                target="_blank",
            ),
            Br(),
            Small(row["description"] or "", cls="muted"),
        ),
        Td(row["provider_activity_type"]),
        Td(format_metric(row["distance_meters"], 1000, " km", 2)),
        Td(format_metric(duration, 60, " min", 1)),
        Td(format_metric(row["pace_seconds_per_kilometer"], 60, " min/km", 2)),
    )


@rt("/athlete/{athlete_id}")
def get_athlete(athlete_id: str, page: int = 1):
    athlete = repository.athlete(athlete_id)
    if athlete is None:
        return "Athlete not found", 404
    stats = repository.snapshot_stats()
    performances = repository.season_bests(athlete_id)
    activity_page = repository.activities(athlete_id, page=page, page_size=30)
    accounts = repository.external_accounts(athlete_id)
    strava = next((row for row in accounts if row["provider"] == "strava"), None)
    display_name = athlete["display_name"] or athlete["official_name"]
    previous_link = f"/athlete/{athlete_id}?page={page - 1}"
    next_link = f"/athlete/{athlete_id}?page={page + 1}"
    return Titled(
        f"{display_name} — EnduranceViz 2024",
        Main(
            A("← Athlete search", href="/"),
            snapshot_banner(stats),
            H1(
                A(display_name, href=f"https://strava.com/athletes/{strava['external_account_id']}", target="_blank")
                if strava else display_name
            ),
            P(
                f"{athlete['nationality_code'] or 'Nationality unknown'} · "
                f"{athlete['primary_discipline'] or 'No primary 2024 discipline'} · "
                f"{athlete['coverage_status'] or 'unknown'} coverage "
                f"({athlete['coverage_score'] or 0:.1f}/100, {athlete['observed_weeks'] or 0} observed full weeks)",
                cls="muted",
            ),
            H2("2024 season bests"),
            Div(
                *[
                    Div(
                        Strong(row["discipline"]),
                        Div(row["mark_text"]),
                        Small(f"{row['performance_date']} · {row['location'] or 'venue unavailable'}"),
                        cls="season-card",
                    )
                    for row in performances
                ],
                cls="season-grid",
            ) if performances else P("No canonical 2024 performance rows."),
            H2("Coverage-aware training summary"),
            Div(
                Div(Strong(format_metric(athlete["total_run_distance_meters"], 1000, " km")), Span("total run distance"), cls="metric"),
                Div(Strong(format_metric(athlete["total_run_duration_seconds"], 3600, " h")), Span("total run duration"), cls="metric"),
                Div(Strong(format_metric(athlete["average_run_distance_per_observed_week_meters"], 1000, " km")), Span("per observed week"), cls="metric"),
                Div(Strong(format_metric(athlete["average_run_distance_per_calendar_week_meters"], 1000, " km")), Span("per 366/7 calendar week"), cls="metric"),
                Div(Strong(format_metric(athlete["weighted_run_pace_seconds_per_kilometer"], 60, " min/km", 2)), Span("weighted public-run pace"), cls="metric"),
                Div(Strong(f"{athlete['total_activity_count'] or 0:,}"), Span("unique activities"), cls="metric"),
                cls="metric-grid",
            ),
            P(
                "Metrics cover 2024-01-01T00:00:00Z through (but not including) 2025-01-01T00:00:00Z. "
                "Missing collection is never interpreted as zero training.",
                cls="muted",
            ),
            H2("2024 activities"),
            Div(
                Table(
                    Thead(Tr(Th("Date (UTC)"), Th("Activity"), Th("Type"), Th("Distance"), Th("Duration"), Th("Pace"))),
                    Tbody(*[activity_row(row) for row in activity_page["rows"]]),
                ),
                cls="table-wrap",
            ) if activity_page["rows"] else P("No public 2024 activities in the canonical snapshot."),
            Div(
                A("← Previous 30", href=previous_link) if activity_page["has_previous"] else Span(""),
                Span(f"Page {activity_page['page']}"),
                A("Next 30 →", href=next_link) if activity_page["has_next"] else Span(""),
                cls="pagination",
            ),
            cls="container",
        ),
    )


if __name__ == "__main__":
    serve(host="0.0.0.0", port=8000)


export = app
