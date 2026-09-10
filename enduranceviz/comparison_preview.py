"""Server-rendered exploratory comparison using the already approved metrics."""

from html import escape
from statistics import median

from fasthtml.common import (A, Button, Details, Div, Form, H1, H2, H3, Label,
                             Main, Option, P, Select, Style, Summary, Table,
                             Tbody, Td, Th, Thead, Tr, NotStr)

from enduranceviz.recorded_training import STUDY_EVENTS
from enduranceviz.page_metadata import metadata, is_synthetic

METRICS = {
    'distance': ('Running distance (km)', 'median_recorded_week_run_distance_meters', 'distance_measured_weeks', 1000),
    'records': ('Run-record count', 'median_recorded_week_run_count', 'recorded_run_weeks', 1),
}

CSS = """
.comparison-page { padding-top: 2rem; isolation: isolate; font-variant-numeric: tabular-nums; }
.comparison-page h1,.comparison-page h2,.comparison-page h3 { font-weight: 600; text-wrap: balance; }
.comparison-page h1 { font-size: clamp(1.8rem,4vw,2.5rem); margin: 1.5rem 0 .75rem; }
.comparison-page h2 { font-size: 1.35rem; margin: 1.5rem 0 .5rem; }
.comparison-page h3 { font-size: 1.1rem; margin: 0 0 .5rem; }
.comparison-page p { max-width: 80ch; text-wrap: pretty; }
.comparison-controls-wrap,.comparison-panels-wrap { container-type: inline-size; }
.comparison-controls { display: grid; grid-template-columns: 1fr; gap: .75rem; align-items: end; margin: 1.5rem 0; }
.comparison-controls label,.comparison-controls select,.comparison-controls button { margin: 0; min-width: 0; font-size: 1rem; }
.comparison-controls select,.comparison-controls button { min-height: 48px; }
.comparison-panels { display: grid; grid-template-columns: minmax(0,1fr); gap: 2rem; }
.comparison-panel { min-width: 0; border-top: 1px solid #8192a340; padding-top: 1rem; }
.comparison-scroll { overflow-x: auto; }
.comparison-chart { display: block; width: 100%; min-width: 470px; color: var(--color); }
.comparison-chart text { fill: currentColor; font-size: 16px; }
.comparison-chart .grid { stroke: currentColor; opacity: .15; }
.comparison-chart .dot { fill: #087f99; stroke: var(--background-color); stroke-width: 1; }
.comparison-chart .iqr { stroke: currentColor; stroke-width: 4; }
.comparison-chart .median { fill: #c25718; stroke: var(--background-color); }
.comparison-chart a:hover .dot { stroke: currentColor; stroke-width: 3; }
.comparison-page :focus-visible { outline: 2px solid var(--primary); outline-offset: 2px; }
.comparison-page summary { min-height: 48px; padding: .65rem 0; }
.comparison-page table { width: 100%; margin-bottom: 0; }
.comparison-page th { white-space: nowrap; }
.comparison-page td:first-child { white-space: nowrap; min-width: 18ch; }
.comparison-page td,.comparison-page th { font-size: 1rem; }
@container (min-width: 650px) {
 .comparison-controls { grid-template-columns: repeat(2,minmax(0,1fr)); }
 .comparison-controls button { width: auto; justify-self: start; }
}
@container (min-width: 950px) {
 .comparison-controls { grid-template-columns: 1fr 1fr 1.3fr 1.3fr auto; }
 .comparison-panels { grid-template-columns: repeat(2,minmax(0,1fr)); }
}
@media (prefers-color-scheme: dark) {
 .comparison-chart .dot { fill: #59c7dd; }
 .comparison-chart .median { fill: #ffa76b; }
}
"""


def quantile(values, fraction):
    """Linear interpolation, matching the static figure's numpy.quantile default."""
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def metric_rows(rows, metric, view):
    _, field, weeks, divisor = METRICS[metric]
    return [dict(r, value=float(r[field]) / divisor, weeks=r[weeks]) for r in rows
            if r[field] is not None
            and (view != 'points' or r['best_stored_results_score'] is not None)]


def chart(rows, view, maximum, score_bounds, label, chart_id):
    """Shared axes; deterministic display-only jitter. Exact values in HTML table."""
    left, right, top, bottom = 55, 445, 24, 224
    parts = [f'<svg class="comparison-chart" viewBox="0 0 470 290" role="img" aria-labelledby="{chart_id}">',
             f'<title id="{chart_id}">{escape(label)}. Each dot is one athlete. Exact values and profile links follow in the table.</title>']
    def text(x, y, value, anchor='middle'):
        return f'<text x="{x}" y="{y}" text-anchor="{anchor}">{escape(str(value))}</text>'
    if view == 'points':
        parts.append(text(left, 16, label + ' / recorded week', 'start'))
    for i in range(5):
        value = maximum * i / 4
        if view == 'distribution':
            x = left + (right-left)*i/4
            parts += [f'<path class="grid" d="M{x} {top} V{bottom}"/>', text(x, 249, f'{value:.1f}')]
        else:
            y = bottom - (bottom-top)*i/4
            parts += [f'<path class="grid" d="M{left} {y} H{right}"/>', text(48, y+5, f'{value:.1f}', 'end')]
    if view == 'points':
        low, high = score_bounds
        for i in range(4):
            parts.append(text(left+(right-left)*i/3, 249, f'{low+(high-low)*i/3:.0f}'))
    for i, row in enumerate(rows):
        if view == 'distribution':
            x = left + row['value']/maximum*(right-left)
            y = 120 + ((i*37 % 71)-35)*1.5
        else:
            x = left + (row['best_stored_results_score']-low)/(high-low)*(right-left)
            y = bottom - row['value']/maximum*(bottom-top)
        detail = (f"{row['name']}: {row['value']:g}; {row['weeks']} contributing weeks; "
                  f"{row['source_warning_run_weeks']} source-warning running weeks; "
                  f"points {row['best_stored_results_score'] if row['best_stored_results_score'] is not None else 'unavailable'}")
        # Table links provide keyboard access without hundreds of tiny SVG tab stops.
        parts.append(f'<a href="/athlete/{escape(row["athlete_id"], quote=True)}" tabindex="-1">'
                     f'<title>{escape(detail)}</title><circle class="dot" cx="{x:.3f}" cy="{y:.3f}" r="4.5"/></a>')
    if rows and view == 'distribution':
        values = [r['value'] for r in rows]
        q1, mid, q3 = [left + quantile(values, q)/maximum*(right-left) for q in (.25,.5,.75)]
        parts += [f'<path class="iqr" d="M{q1} 120 H{q3}"/>',
                  f'<path class="median" d="M{mid} 113 l7 7 -7 7 -7 -7 Z"/>']
    if not rows:
        parts.append(text(250,120,'No contributing observations'))
    parts.append(text(250,278,'Primary-event result points' if view == 'points' else label))
    parts.append('</svg>')
    return NotStr(''.join(parts))


def comparison_page(repository, banner, first='800m', second='5000m', metric='distance', view='distribution'):
    if first not in STUDY_EVENTS or second not in STUDY_EVENTS or metric not in METRICS or view not in ('distribution','points'):
        raise ValueError('Choose a listed event, metric, and view.')
    events = list(dict.fromkeys((first, second)))
    source = [r for r in repository.comparison_athletes() if r['primary_discipline'] in events]
    shown = metric_rows([r for r in source if r['gender'] in ('female','male')],metric,view)
    maximum = max(1, max((r['value'] for r in shown), default=1)) * 1.08
    scores = [r['best_stored_results_score'] for r in shown if r['best_stored_results_score'] is not None]
    bounds = (min(scores)-10, max(scores)+10) if scores else (1100,1200)
    label = METRICS[metric][0]
    def select(name, title, options, selected):
        return Div(Label(title, fr=f'compare-{name}'),
                   Select(*(Option(text, value=value, selected=value==selected) for value,text in options),
                          name=name, id=f'compare-{name}'))
    panels = []
    for event_index,event in enumerate(events):
        for sex,title in (('female','Women'),('male','Men')):
            inventory = [r for r in source if r['primary_discipline']==event and r['gender']==sex]
            group = [r for r in shown if r['primary_discipline']==event and r['gender']==sex]
            weeks = f"{median(r['weeks'] for r in group):g}" if group else 'unavailable'
            values = [r['value'] for r in group]
            summary = (f"Median {median(values):.1f} · middle 50% {quantile(values,.25):.1f}–{quantile(values,.75):.1f}."
                       if values else 'No metric available for this panel.')
            table = Table(Thead(Tr(*(Th(t,scope='col') for t in ('Athlete','Value','Contributing weeks','Run weeks','Warning run weeks','Points')))),
                Tbody(*(Tr(Td(A(r['name'],href=f"/athlete/{r['athlete_id']}")),Td(f"{r['value']:.2f}"),
                           Td(f"{r['weeks']}/52"),Td(r['recorded_run_weeks']),Td(r['source_warning_run_weeks']),
                           Td(r['best_stored_results_score'] if r['best_stored_results_score'] is not None else 'Unavailable')) for r in group)))
            panels.append(Div(H3(f'{event} · {title}'),
                P(f"{len(group)} contributors / {len(inventory)} registry athletes · median {weeks}/52 contributing weeks."),
                Div(chart(group,view,maximum,bounds,label,f'chart-{event_index}-{sex}'),
                    cls='comparison-scroll',tabindex='0',role='region',aria_label=f'{event} {title} chart'),
                P(summary),
                P(f"{sum(r['source_warning_run_weeks']>0 for r in group)} contributors have source-warning running weeks. "
                  f"{len(inventory)-len(group)} registry athletes lack the metric" + (' or primary-event score.' if view=='points' else '.')),
                Details(Summary('Inspect values and contributing weeks'),
                    Div(table,cls='comparison-scroll',tabindex='0',role='region',aria_label=f'{event} {title} values')),
                cls='comparison-panel'))
    stats = repository.snapshot_stats()
    synthetic = is_synthetic(stats)
    return (*metadata('Recorded running comparisons — EnduranceViz 2024' + (' · Synthetic Demo' if synthetic else ''),
                      'Exploratory athlete-level recorded-week comparisons. Contributing weeks shown; not complete training histories.'
                      if not synthetic else 'Invented software-test comparisons. Not real athletes or training data.',synthetic),
        Style(CSS), Main(P(A('Back to athlete explorer',href='/')),banner(stats),
            H1('How does recorded running compare?'),
            P('Explore event differences and performance points in the 2024 snapshot. Each dot is one athlete’s median across full weeks with recorded Runs.'),
            Div(Form(select('first','First event',[(e,e) for e in STUDY_EVENTS],first),
                     select('second','Second event',[(e,e) for e in STUDY_EVENTS],second),
                     select('metric','Weekly metric',[(k,v[0]) for k,v in METRICS.items()],metric),
                     select('view','Comparison view',[('distribution','Event distributions'),('points','Performance points')],view),
                     Button('Update',type='submit'),action='/compare',method='get',cls='comparison-controls'),cls='comparison-controls-wrap'),
            P('Exploratory · no minimum-week cutoff · women and men shown separately.'),
            P('Distance: km per recorded week. Frequency: stored Run records, not training sessions. '
              'Orange diamond = group median; line = middle 50% of athlete medians, not a confidence interval.'
              if view=='distribution' else f'Vertical axis: {label.lower()} per recorded week. Horizontal axis: highest stored 2024 primary-event result points. No fitted line or pooled statistic.'),
            Div(Div(*panels,cls='comparison-panels'),cls='comparison-panels-wrap'),
            H2('Definitions and limits'),
            P('Weeks run Monday–Sunday in UTC. Medians use the 52 full weeks of 2024; December 30–31 is excluded. '
              'Distance requires every Run distance in a week to be present, finite, and nonnegative. '
              'Run-record counts use all full weeks with recorded Runs. Missing weeks are not zero training.'),
            P('All metric-available contributors remain, including sparse posting and source-warning weeks. '
              'Warning counts cover all recorded-running weeks, not only distance-measured weeks. '
              'Contributing-week counts describe recording breadth, not confidence. Small panels describe only the displayed athletes.'),
            P('One athlete appears once, in their existing primary event. Points are the highest stored result score within that event, '
              'with no other-event fallback. Primary-event assignment itself uses points; the source has a pre-existing 1,100-point floor. '
              'Public posting is selective, so these are not complete training histories or causal comparisons.'),
            P(f"Other/unknown recorded-sex registry athletes in these events: {sum(r['gender'] not in ('female','male') for r in source)}; not pooled into these panels. "
              'Panels use shared axes for the selected comparison. Exact athlete values and week counts are in the expandable tables.'),
            cls='container comparison-page'))
