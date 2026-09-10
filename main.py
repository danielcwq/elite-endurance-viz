"""FastHTML application for the fixed EnduranceViz 2024 snapshot."""

from __future__ import annotations

import os
import duckdb
from datetime import datetime, timezone
from functools import lru_cache

from fasthtml.common import *
from starlette.responses import JSONResponse, PlainTextResponse
from starlette.middleware import Middleware

from enduranceviz.geography import COUNTRY_CENTROIDS, NON_GEOGRAPHIC_CODES
from enduranceviz.activity_filters import ACTIVITY_FILTER_CSS, ActivityFilters, activity_filter_form
from enduranceviz.serving import ServingRepository
from enduranceviz.training_profile import TRAINING_PROFILE_CSS, training_section
from enduranceviz.operations import RequestLogMiddleware
from enduranceviz.recorded_training import POLICY_VERSION


repository = ServingRepository()

app, rt = fast_app(
    middleware=[Middleware(RequestLogMiddleware)],
    # FastHTML otherwise writes a generated key to .sesskey during import,
    # which is incompatible with Vercel's read-only function filesystem.
    secret_key=os.getenv("ENDURANCEVIZ_SESSION_SECRET", "enduranceviz-2024-read-only-snapshot"),
    hdrs=(
        Style(TRAINING_PROFILE_CSS),
        Style(ACTIVITY_FILTER_CSS),
        Link(rel="stylesheet", href="https://cdn.jsdelivr.net/npm/@picocss/pico@1/css/pico.min.css"),
        Link(
            rel="stylesheet",
            href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css",
            integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=",
            crossorigin="anonymous",
        ),
        Script(
            src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js",
            integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=",
            crossorigin="anonymous",
        ),
        Style(
            """
            :root { --max-width: 1120px; }
            body { padding-bottom: 4rem; }
            .home-page { padding-top: 2rem; padding-bottom: 1rem; }
            .home-brand { margin: 0 0 1.5rem; font-weight: 700; font-size: 1.1rem; }
            .home-page h1 { margin: 1.5rem 0 .75rem; font-size: clamp(1.8rem, 4vw, 2.5rem); line-height: 1.15; max-width: 800px; }
            .home-intro { max-width: 700px; margin-bottom: 1.25rem; }
            .home-page h2 { margin: 2rem 0 1rem; font-size: 1.35rem; }
            .home-page .metric { border-color: #cbd5df; }
            .home-page .metric span { font-size: .875rem; }
            .home-note { max-width: 800px; }
            .home-page .map-panel h2 { margin-top: 0; }
            .snapshot-banner { background: #eef6ff; border: 1px solid #b9d7f5; padding: .75rem 1rem; border-radius: .5rem; }
            .explorer-controls { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: .75rem; max-width: 920px; align-items: start; }
            .search-wrap { position: relative; min-width: 0; }
            .map-toggle { white-space: nowrap; width: auto; margin: 0; }
            .search-results { position: absolute; inset: 100% 0 auto; z-index: 10; background: white; border: 1px solid #ddd; border-radius: .4rem; box-shadow: 0 8px 20px #0002; }
            .search-results a { display: block; padding: .7rem 1rem; text-decoration: none; border-bottom: 1px solid #eee; }
            .search-results a:last-child { border-bottom: 0; }
            .map-panel { margin-top: 1.5rem; padding: 1rem; border: 1px solid #cbd5df; border-radius: .5rem; }
            .map-panel[hidden] { display: none; }
            .map-panel header { margin-bottom: 1rem; }
            .map-panel header h2 { margin-bottom: .25rem; }
            .map-layout { display: grid; grid-template-columns: minmax(0, 2fr) minmax(260px, 1fr); gap: 1rem; }
            #country-map { min-height: 480px; border: 1px solid #cbd5df; border-radius: .4rem; overflow: hidden; }
            .map-sidebar label { margin-bottom: .75rem; }
            .map-sidebar select { margin-top: .35rem; }
            .map-details { max-height: 370px; overflow-y: auto; padding-right: .25rem; }
            .map-details h3 { margin-bottom: .2rem; }
            .athlete-roster { list-style: none; margin: .75rem 0 0; padding: 0; }
            .athlete-roster li { padding: .55rem 0; border-top: 1px solid #e3e8ed; }
            .athlete-roster small { display: block; }
            .map-legend { margin: .65rem 0 0; font-size: .875rem; }
            .metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1rem; }
            .metric { padding: 1rem; border: 1px solid #ddd; border-radius: .5rem; }
            .metric strong { display: block; font-size: 1.35rem; }
            .muted { color: #667085; }
            .season-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: .75rem; }
            .season-card { border: 1px solid #ddd; border-radius: .5rem; padding: .8rem; }
            .table-wrap { overflow-x: auto; }
            .pagination { display: flex; gap: 1rem; align-items: center; justify-content: center; }
            @media (max-width: 720px) {
              .explorer-controls, .map-layout { grid-template-columns: 1fr; }
              .map-toggle { width: 100%; }
              #country-map { min-height: 360px; }
              .map-details { max-height: none; }
            }
            @media (prefers-color-scheme: dark) {
              .muted { color: #9cabb8; }
              .home-page .metric { border-color: #394956; background: #13202a; }
              .snapshot-banner { background: #102b43; border-color: #28628f; }
              .search-results { background: #18232d; border-color: #394956; }
              .search-results a { border-color: #394956; }
              .map-panel, #country-map { border-color: #394956; }
              .athlete-roster li { border-color: #394956; }
            }
            """
        ),
    ),
)

# Vercel's Python entrypoint scanner only recognizes simple top-level names.
# Keep FastHTML's ``app, rt`` construction for local development while exposing
# an explicit ASGI application for the deployment runtime.
application = app


@rt('/health', methods=['GET'])
def health():
    try:
        metadata = repository.health_metadata()
    except (OSError, duckdb.Error):
        metadata = None
    if metadata is None:
        return JSONResponse({'status': 'unavailable', 'component': 'snapshot'},
                            status_code=503, headers={'Cache-Control': 'no-store'})
    return JSONResponse({
        'status': 'ok', 'dataset_version': metadata['dataset_version'],
        'dataset_built_at_utc': metadata['build_time'].astimezone(timezone.utc).isoformat(),
        'analytics_policy': POLICY_VERSION,
    }, headers={'Cache-Control': 'no-store'})


def format_timestamp(value) -> str:
    if value is None:
        return "Unknown"
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc)
    return value.strftime("%Y-%m-%d %H:%M UTC")


def format_metric(value, divisor: float = 1, suffix: str = "", decimals: int = 1) -> str:
    if value is None:
        return "Missing"
    return f"{float(value) / divisor:,.{decimals}f}{suffix}"


def snapshot_banner(stats: dict) -> Div:
    synthetic = str(stats['dataset_version']).startswith('SYNTHETIC-DEMO')
    return Div(
        Strong("Synthetic demo — not real athletes" if synthetic else "2024 Snapshot"),
        " · ",
        Span(f"dataset v{stats['dataset_version']}"),
        " · ",
        Span(f"built {format_timestamp(stats['build_time'])}"),
        Br(),
        Small("All athletes, results, and activities here are invented software-test examples." if synthetic
              else "Publicly observed Strava activity, not a complete or current training history."),
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


@rt("/api/map/countries")
def map_countries():
    countries = []
    non_geographic = []
    for row in repository.map_countries():
        code = row["nationality_code"]
        if code in NON_GEOGRAPHIC_CODES:
            non_geographic.append(row)
            continue
        geography = COUNTRY_CENTROIDS.get(code)
        if geography is None:
            continue
        country_name, latitude, longitude = geography
        countries.append(
            {
                **row,
                "country_name": country_name,
                "latitude": latitude,
                "longitude": longitude,
            }
        )
    return JSONResponse({"countries": countries, "non_geographic": non_geographic})


@rt("/api/map/countries/{country_code}/athletes")
def map_country_athletes(country_code: str):
    code = country_code.strip().upper()
    if code not in COUNTRY_CENTROIDS:
        return PlainTextResponse("Nationality code not found", status_code=404)
    return JSONResponse(list(repository.map_country_athletes(code)))


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
    return (
        Title("EnduranceViz — 2024 Snapshot"),
        Main(
            P("EnduranceViz", cls="home-brand"),
            snapshot_banner(stats),
            H1("Elite endurance training, observed in 2024"),
            P(
                "Explore elite runners, their 2024 performances, and publicly observed training. "
                "Search for an athlete or browse the map by nationality.",
                cls="muted home-intro",
            ),
            Div(
                Div(
                    Input(
                        type="search",
                        id="athlete-search",
                        placeholder="Search an athlete…",
                        autocomplete="off",
                        aria_label="Search the athlete registry",
                    ),
                    Div(id="search-results", cls="search-results", hidden=True),
                    cls="search-wrap",
                ),
                Button(
                    "Explore map",
                    type="button",
                    id="map-toggle",
                    cls="secondary outline map-toggle",
                    aria_controls="map-panel",
                    aria_expanded="false",
                ),
                cls="explorer-controls",
            ),
            Section(
                Header(
                    H2("Athletes by nationality"),
                    P(
                        "Explore the 2024 athlete registry by competition affiliation. Points use country centroids—not residence or training locations.",
                        cls="muted",
                    ),
                ),
                Div(
                    Div(id="country-map", role="region", aria_label="Athletes grouped by nationality on a world map"),
                    Aside(
                        Label(
                            "Country or affiliation",
                            Select(
                                Option("Choose a country…", value=""),
                                id="country-select",
                                name="country",
                            ),
                        ),
                        Div(
                            P("Select a point or country to see athletes in the 2024 snapshot.", cls="muted"),
                            id="map-details",
                            cls="map-details",
                            aria_live="polite",
                        ),
                        P("Circle size represents athlete count.", cls="muted map-legend"),
                        cls="map-sidebar",
                    ),
                    cls="map-layout",
                ),
                id="map-panel",
                cls="map-panel",
                hidden=True,
            ),
            Script(
                """
                const input = document.getElementById('athlete-search');
                const output = document.getElementById('search-results');
                const mapToggle = document.getElementById('map-toggle');
                const mapPanel = document.getElementById('map-panel');
                const countrySelect = document.getElementById('country-select');
                const mapDetails = document.getElementById('map-details');
                let timer;

                const escapeHtml = value => {
                  const element = document.createElement('span');
                  element.textContent = value == null ? '' : String(value);
                  return element.innerHTML;
                };

                input.addEventListener('input', () => {
                  clearTimeout(timer);
                  timer = setTimeout(async () => {
                    const query = input.value.trim();
                    if (query.length < 2) { output.hidden = true; output.innerHTML = ''; return; }
                    const rows = await fetch('/api/athletes/search?q=' + encodeURIComponent(query)).then(r => r.json());
                    output.innerHTML = rows.length ? rows.map(row =>
                      `<a href="/athlete/${encodeURIComponent(row.athlete_id)}"><strong>${escapeHtml(row.display_name)}</strong><br>` +
                      `<small>${escapeHtml(row.nationality_code || '—')} · ${escapeHtml(row.primary_discipline || 'No 2024 discipline')} · ${escapeHtml(row.coverage_status || 'unknown')} coverage</small></a>`
                    ).join('') : '<a>No athletes found</a>';
                    output.hidden = false;
                  }, 180);
                });
                document.addEventListener('click', event => {
                  if (!output.contains(event.target) && event.target !== input) output.hidden = true;
                });

                let countryMap;
                let countryRows;
                let mapLoaded = false;

                function showMapMessage(message) {
                  mapDetails.replaceChildren();
                  const paragraph = document.createElement('p');
                  paragraph.className = 'muted';
                  paragraph.textContent = message;
                  mapDetails.append(paragraph);
                }

                async function showCountry(code) {
                  if (!code) {
                    showMapMessage('Select a point or country to see athletes in the 2024 snapshot.');
                    return;
                  }
                  const country = countryRows.find(row => row.nationality_code === code);
                  if (!country) return;
                  countrySelect.value = code;
                  showMapMessage(`Loading ${country.country_name}…`);
                  try {
                    const response = await fetch(`/api/map/countries/${encodeURIComponent(code)}/athletes`);
                    if (!response.ok) throw new Error('Unable to load athletes');
                    const athletes = await response.json();
                    mapDetails.replaceChildren();

                    const heading = document.createElement('h3');
                    heading.textContent = `${country.country_name} (${code})`;
                    const summary = document.createElement('p');
                    summary.className = 'muted';
                    summary.textContent = `${country.athlete_count.toLocaleString()} athletes · ${country.observed_athlete_count.toLocaleString()} with observed 2024 activity`;
                    const list = document.createElement('ul');
                    list.className = 'athlete-roster';
                    athletes.forEach(athlete => {
                      const item = document.createElement('li');
                      const link = document.createElement('a');
                      link.href = `/athlete/${encodeURIComponent(athlete.athlete_id)}`;
                      link.textContent = athlete.display_name;
                      const metadata = document.createElement('small');
                      metadata.className = 'muted';
                      metadata.textContent = `${athlete.primary_discipline || 'No 2024 discipline'} · ${athlete.coverage_status || 'unknown'} coverage`;
                      item.append(link, metadata);
                      list.append(item);
                    });
                    mapDetails.append(heading, summary, list);
                  } catch (error) {
                    showMapMessage('The athlete roster could not be loaded. Please try again.');
                  }
                }

                async function initializeMap() {
                  if (mapLoaded) {
                    countryMap.invalidateSize();
                    return;
                  }
                  mapLoaded = true;
                  if (!window.L) {
                    showMapMessage('The interactive map could not be loaded. Search remains available above.');
                    return;
                  }
                  showMapMessage('Loading nationality summary…');
                  try {
                    const response = await fetch('/api/map/countries');
                    if (!response.ok) throw new Error('Unable to load map summary');
                    const payload = await response.json();
                    countryRows = payload.countries;
                    countryMap = L.map('country-map', { worldCopyJump: true, minZoom: 2 }).setView([18, 8], 2);
                    L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
                      maxZoom: 19,
                      attribution: '&copy; OpenStreetMap contributors',
                    }).addTo(countryMap);

                    [...countryRows]
                      .sort((a, b) => a.country_name.localeCompare(b.country_name))
                      .forEach(country => {
                        const option = document.createElement('option');
                        option.value = country.nationality_code;
                        option.textContent = `${country.country_name} (${country.athlete_count})`;
                        countrySelect.append(option);
                      });

                    countryRows.forEach(country => {
                      const radius = Math.max(5, Math.min(20, 4 + Math.sqrt(country.athlete_count) * 1.5));
                      const marker = L.circleMarker([country.latitude, country.longitude], {
                        radius,
                        color: '#0b72b9',
                        weight: 1.5,
                        fillColor: '#23a6f0',
                        fillOpacity: 0.72,
                      }).addTo(countryMap);
                      marker.bindTooltip(`${escapeHtml(country.country_name)} · ${country.athlete_count.toLocaleString()} athletes`);
                      marker.on('click', () => showCountry(country.nationality_code));
                    });
                    showMapMessage('Select a point or country to see athletes in the 2024 snapshot.');
                    setTimeout(() => countryMap.invalidateSize(), 0);
                  } catch (error) {
                    mapLoaded = false;
                    showMapMessage('The nationality summary could not be loaded. Please try again.');
                  }
                }

                mapToggle.addEventListener('click', () => {
                  const opening = mapPanel.hidden;
                  mapPanel.hidden = !opening;
                  mapToggle.setAttribute('aria-expanded', String(opening));
                  mapToggle.textContent = opening ? 'Hide map' : 'Explore map';
                  if (opening) initializeMap();
                });
                countrySelect.addEventListener('change', () => showCountry(countrySelect.value));
                """
            ),
            H2("Inside the 2024 snapshot"),
            Div(
                Div(Strong(f"{stats['athlete_count']:,}"), Span("athletes"), cls="metric"),
                Div(Strong(f"{stats['strava_account_count']:,}"), Span("linked Strava accounts"), cls="metric"),
                Div(Strong(f"{stats['activity_count']:,}"), Span("unique 2024 activities"), cls="metric"),
                Div(Strong(f"{stats['country_count']:,}"), Span("nationality / affiliation codes"), cls="metric"),
                cls="metric-grid",
            ),
            H2("What the numbers mean"),
            P(
                "Training coverage varies by athlete. A week with no collected data does not mean "
                "an athlete did no training. Totals include unique activities observed in 2024; "
                "they may not represent an athlete’s complete training history.",
                cls="home-note muted",
            ),
            P(A("Dataset methods and limitations", href="https://github.com/danielcwq/elite-endurance-viz/blob/p0-2024-data-foundation/docs/data-specification-2024-v1.md")),
            cls="container home-page",
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
def get_athlete(athlete_id: str, page: str = '1', start: str = '', end: str = '', category: str = 'All'):
    athlete = repository.athlete(athlete_id)
    if athlete is None:
        return PlainTextResponse("Athlete not found", status_code=404)
    try:
        filters = ActivityFilters.parse(start, end, category)
        page_number = int(page)
        if page_number < 1:
            raise ValueError('Page must be a positive integer')
    except ValueError as exc:
        return PlainTextResponse(f'Invalid activity filters: {exc}. Edit the URL or return to /athlete/{athlete_id}.', status_code=400)
    stats = repository.snapshot_stats()
    performances = repository.season_bests(athlete_id)
    activity_page = repository.activities(athlete_id, page=page_number, page_size=30, filters=filters)
    accounts = repository.external_accounts(athlete_id)
    weeks = repository.recorded_weeks(athlete_id)
    strava = next((row for row in accounts if row["provider"] == "strava"), None)
    display_name = athlete["display_name"] or athlete["official_name"]
    previous_link = filters.url(athlete_id, activity_page['page'] - 1)
    next_link = filters.url(athlete_id, activity_page['page'] + 1)
    return (
        Title(f"{display_name} — EnduranceViz 2024"),
        Main(
            A("← Athlete search", href="/"),
            snapshot_banner(stats),
            H1(
                A(display_name, href=f"https://strava.com/athletes/{strava['external_account_id']}", target="_blank")
                if strava else display_name
            ),
            P(
                f"{athlete['nationality_code'] or 'Nationality unknown'} · "
                f"{athlete['primary_discipline'] or 'No primary 2024 discipline'}",
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
            training_section(weeks),
            Section(
                H2("2024 activities"),
                *activity_filter_form(athlete_id, filters),
                P(f"Showing {activity_page['first']:,}–{activity_page['last']:,} of {activity_page['total']:,} matching activity records."
                  if activity_page['total'] else 'No stored activities match these filters. This does not imply no training.'),
                Div(
                    Div(
                        Table(
                            Thead(Tr(*[Th(label, scope='col') for label in
                                       ('Date (UTC)', 'Activity', 'Type', 'Distance', 'Duration', 'Pace')])),
                            Tbody(*[activity_row(row) for row in activity_page["rows"]]),
                        ),
                        cls="activity-table-inner",
                    ),
                    cls="table-wrap", tabindex='0', role='region', aria_label='Filtered activity records',
                ) if activity_page["rows"] else None,
                Nav(
                    A("← Previous 30", href=previous_link) if activity_page["has_previous"] else Span(""),
                    Span(f"Page {activity_page['page']} of {activity_page['total_pages']}"),
                    A("Next 30 →", href=next_link) if activity_page["has_next"] else Span(""),
                    cls="pagination", aria_label='Activity pages',
                ) if activity_page['total'] else None,
                cls='activity-section', id='activities',
            ),
            cls="container",
        ),
    )


if __name__ == "__main__":
    serve(host="0.0.0.0", port=8000)


export = app
