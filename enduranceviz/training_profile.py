"""Server-rendered P1 timelines with equivalent accessible weekly data."""

from datetime import date, timedelta
from html import escape
from math import ceil
from statistics import median

from fasthtml.common import (
    Caption, Details, Div, H2, H3, NotStr, P, Section, Small, Span, Strong,
    Summary, Table, Tbody, Td, Th, Thead, Tr,
)

from enduranceviz.recorded_training import POLICY_VERSION

TRAINING_PROFILE_CSS = """
.training-profile { margin: 2rem 0; container-type: inline-size; }
.training-profile h2, .training-profile h3 { font-weight: 600; text-wrap: balance; }
.training-profile h2 { font-size: 1.6rem; letter-spacing: -.02em; margin-bottom: .6rem; }
.training-profile h3 { font-size: 1.1rem; margin: 0 0 .4rem; }
.training-profile p { max-width: 78ch; text-wrap: pretty; }
.training-profile { --chart-accent: #087b98; --chart-warning: #976000; --chart-grid: #152c3c20; --chart-gap: #152c3c0b; }
.training-stats { display: grid; grid-template-columns: 1fr; gap: 1.25rem; margin: 1.5rem 0; }
.training-stat { min-width: 0; }
.training-stat p { margin: 0; font-size: 1rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.training-stat .training-value { font-size: 1.75rem; font-weight: 600; font-variant-numeric: tabular-nums; }
.training-stat .training-denominator { white-space: normal; color: var(--muted-color); }
.training-legend { display: flex; flex-wrap: wrap; gap: .5rem 1.2rem; margin: 1rem 0; }
.training-legend p { margin: 0; font-size: 1rem; }
.training-source-note { border-left: 3px solid var(--chart-warning); padding-left: .9rem; }
.training-chart { margin: 1.75rem 0; }
.training-chart-scroll { overflow-x: auto; max-width: 100%; }
.training-chart-scroll:focus-visible { outline: 2px solid var(--primary); outline-offset: 2px; }
.training-svg { display: block; width: 100%; min-width: 660px; height: auto; }
.training-scroll-hint { color: var(--muted-color); }
.training-svg text { fill: currentColor; font-family: inherit; font-size: 13px; font-variant-numeric: tabular-nums; }
.training-grid { stroke: var(--chart-grid); stroke-width: 1; }
.training-bar { fill: var(--chart-accent); }
.training-bar.warning, .training-zero.warning { stroke: var(--chart-warning); stroke-width: 2; }
.training-partial { fill: none; stroke: currentColor; stroke-width: 1; stroke-dasharray: 3 3; }
.training-gap { fill: var(--chart-gap); }
.training-gap-mark { stroke: var(--muted-color); stroke-width: 1.5; }
.training-zero { fill: var(--chart-accent); }
.training-profile details { margin-top: 1.5rem; }
.training-table-scroll { overflow-x: auto; margin: -.5rem 0; }
.training-table-inner { min-width: 100%; display: inline-block; padding: .5rem 0; vertical-align: middle; }
.training-profile table { width: 100%; font-size: 1rem; font-variant-numeric: tabular-nums; }
.training-profile th { white-space: nowrap; }
.training-profile td { vertical-align: top; }
.training-profile caption { text-align: left; color: var(--muted-color); padding: .8rem 0; }
.training-profile .week-label { white-space: nowrap; }
@container (min-width: 600px) {
  .training-stats { grid-template-columns: repeat(3,minmax(0,1fr)); gap: 1.5rem; }
  .training-stat p, .training-legend p, .training-profile table { font-size: .875rem; }
  .training-scroll-hint { display: none; }
}
@container (max-width: 599px) {
  .training-svg { min-width: 900px; }
  .training-svg text { font-size: 16px; }
}
@media (prefers-color-scheme: dark) {
  .training-profile { --chart-accent: #4bb9d5; --chart-warning: #efba67; --chart-grid: #ffffff20; --chart-gap: #ffffff08; }
}
"""


def summarize_weeks(weeks):
    full = [row for row in weeks if not row['is_partial_window']]
    running = [row for row in full if row['posted_runs'] > 0]
    distances = [row['recorded_week_run_distance_meters'] for row in running
                 if row['recorded_week_run_distance_meters'] is not None]
    return {
        'full_weeks': len(full), 'run_weeks': len(running), 'distance_weeks': len(distances),
        'distance_median': median(distances) if distances else None,
        'record_median': median([r['posted_runs'] for r in running]) if running else None,
        'day_median': median([r['recorded_run_days'] for r in running]) if running else None,
        'warning_run_weeks': sum(r['evidence_state'] == 'source_warning' for r in running),
    }


def number(value, divisor=1, suffix=''):
    return 'Unavailable' if value is None else f'{value/divisor:,.1f}{suffix}'


def evidence_label(row):
    labels = {
        'activity_and_weekly_record': 'Activity and weekly record',
        'activity_without_weekly_record': 'Activities without a weekly record',
        'ambiguous_empty_record': 'Ambiguous empty collection',
        'source_warning': 'Source warning',
        'no_collection_evidence': 'No collection evidence',
    }
    label = labels[row['evidence_state']]
    if row['runs_missing_distance'] or row['runs_invalid_distance']:
        label += '; Run distance incomplete'
    return label


def timeline_svg(weeks, field, title, divisor=1):
    width, height, left, right, top, bottom = 900, 215, 48, 12, 18, 166
    values = [r[field]/divisor for r in weeks if r[field] is not None]
    maximum = max(values, default=1)
    scale = max(1, ceil(maximum / 4)) * 4
    step = (width-left-right)/max(1,len(weeks))
    out = [f'<svg viewBox="0 0 {width} {height}" role="img" aria-labelledby="{field}-title {field}-desc" class="training-svg">',
        f'<title id="{field}-title">{escape(title)}</title>',
        f'<desc id="{field}-desc">Weekly recorded values for 2024. Gaps are unavailable, not zero training. '
        'Amber outlines mark source warnings. The final dashed column is December 30–31. '
        'Exact values and evidence states are available in the weekly data table below.</desc>']
    for tick in (0,scale/2,scale):
        y=bottom-(bottom-top)*tick/scale
        out += [f'<line x1="{left}" x2="{width-right}" y1="{y}" y2="{y}" class="training-grid"/>',
                f'<text x="{left-8}" y="{y+4}" text-anchor="end">{tick:g}</text>']
    last_month = None
    for index,row in enumerate(weeks):
        x=left+index*step+2
        week=row['week_start_utc']
        # No record of running does not assert a zero training week. Counts of
        # zero remain factual in the table; timelines reserve a gap for these.
        value = row[field] if row['posted_runs'] > 0 else None
        description = f"{week}: {number(value,divisor)}; {evidence_label(row)}"
        if row['is_partial_window']:
            description += '; partial week, December 30–31 only'
            out.append(f'<rect x="{x-1}" y="{top}" width="{step-2}" height="{bottom-top}" class="training-partial"/>')
        out.append(f'<g><title>{escape(description)}</title>')
        if value is None:
            out += [f'<rect x="{x}" y="{top}" width="{step-4}" height="{bottom-top}" class="training-gap"/>',
                    f'<path d="M {x+2} {bottom+10} l {step-8} 0" class="training-gap-mark"/>']
        elif value == 0:
            cls='training-zero warning' if row['evidence_state']=='source_warning' else 'training-zero'
            out.append(f'<circle cx="{x+(step-4)/2}" cy="{bottom}" r="3" class="{cls}"/>')
        else:
            bar_height=(bottom-top)*(value/divisor)/scale
            cls='training-bar warning' if row['evidence_state']=='source_warning' else 'training-bar'
            out.append(f'<rect x="{x}" y="{bottom-bar_height}" width="{step-4}" height="{bar_height}" class="{cls}"/>')
        out.append('</g>')
        if week.month != last_month:
            out.append(f'<text x="{x}" y="{height-9}">{week.strftime("%b")}</text>')
            last_month=week.month
    out.append('</svg>')
    return NotStr(''.join(out))


def training_section(weeks):
    stats=summarize_weeks(weeks)
    has_runs=any(r['posted_runs'] for r in weeks)
    cards = [
        ('Median recorded distance', number(stats['distance_median'],1000,' km'), f"{stats['distance_weeks']} full weeks with measured Run distances"),
        ('Median Run record count', number(stats['record_median']), f"{stats['run_weeks']} full weeks with recorded runs"),
        ('Median recorded run days', number(stats['day_median']), 'Distinct UTC dates, not training sessions'),
    ]
    charts=[]
    if has_runs:
        for field,title,divisor in (
            ('recorded_week_run_distance_meters','Recorded running distance · km',1000),
            ('posted_runs','Run record count · not session count',1),
            ('recorded_run_days','Recorded running days · UTC',1),
        ):
            charts.append(Div(H3(title), Div(timeline_svg(weeks,field,title,divisor),
                cls='training-chart-scroll', tabindex='0', role='region', aria_label=f'{title}. Scroll horizontally on small screens.'), cls='training-chart'))
    else:
        charts.append(P('No Run records are available for this athlete in the 2024 snapshot. Training amounts are unavailable, not zero.', cls='training-source-note'))
    return Section(
        H2('Recorded running in 2024'),
        P(f"{stats['run_weeks']} of {stats['full_weeks']} full weeks contain Run records. These summaries describe recorded running weeks, not a complete training year."),
        Div(*[Div(P(label), P(value,cls='training-value'), P(note,cls='training-denominator'), cls='training-stat') for label,value,note in cards], cls='training-stats'),
        P(f"Source warnings affect {stats['warning_run_weeks']} of the recorded running weeks. Their activity records remain visible. A week without a warning is not proof of complete capture."
          if stats['warning_run_weeks'] else 'No source warnings are flagged in the recorded running weeks. This does not establish complete capture.',
          cls='training-source-note' if stats['warning_run_weeks'] else 'muted') if has_runs else None,
        Div(P('Gaps: unavailable'), P('Amber outline: source warning'), P('Dashed column: Dec 30–31'), cls='training-legend') if has_runs else None,
        P('Scroll horizontally within each chart to view all 53 weeks.',cls='training-scroll-hint') if has_runs else None,
        *charts,
        Details(Summary('Inspect weekly values and source evidence'),
            P('Zero records means no stored runs, not zero training. December 30–31 is excluded from the full-week medians.'),
            Div(Div(Table(
                Caption('Weekly recorded running · all 53 rows'),
                Thead(Tr(*[Th(text,scope='col') for text in ('Week (UTC)','Run distance','Run records','Run days','Evidence')])),
                Tbody(*[Tr(
                    Th(f"{r['week_start_utc'].strftime('%d %b')}–{min(r['week_start_utc']+timedelta(days=6),date(2024,12,31)).strftime('%d %b')}",scope='row',cls='week-label'),
                    Td(number(r['recorded_week_run_distance_meters'],1000,' km')),
                    Td(str(r['posted_runs'])), Td(str(r['recorded_run_days'])),
                    Td(evidence_label(r), Small(f" · {r['collection_error_code']}" if r['collection_error_code'] else ''),
                       Small(' · Partial week' if r['is_partial_window'] else '')),
                ) for r in weeks]),
            ),cls='training-table-inner'), cls='training-table-scroll')),
        Details(Summary('How these metrics are calculated'),
            P('Each full week starts Monday in UTC. The medians use weeks with at least one stored Run. Distance is unavailable if any recorded run has missing or invalid distance. Real recorded zero distances are retained.'),
            P('Run records may include separate warm-ups, repetitions, and cool-downs. Multiple records do not prove multiple sessions. Recorded run days count UTC dates with at least one Run.'),
            P('Source warnings are shown, not silently excluded. Unknown or empty collections are not interpreted as zero training. No annual-week cutoff or P0 eligibility filter is used.'),
            P(f'Calculation policy: {POLICY_VERSION}. Dataset version and build date appear at the top of this profile.',cls='muted')),
        cls='training-profile', id='recorded-training',
    )
