"""Validated, URL-backed filters for the activity table (not training metrics)."""

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from urllib.parse import urlencode

from fasthtml.common import A, Button, Div, Form, Input, Label, Option, P, Select, Span


CATEGORIES = ('All', 'Run', 'Ride', 'Swim', 'Other')
SNAPSHOT_START = date(2024, 1, 1)
SNAPSHOT_END = date(2024, 12, 31)


@dataclass(frozen=True)
class ActivityFilters:
    start: date = SNAPSHOT_START
    end: date = SNAPSHOT_END
    category: str = 'All'

    def __post_init__(self):
        if not SNAPSHOT_START <= self.start <= self.end <= SNAPSHOT_END:
            raise ValueError('Dates must be in 2024, with the start on or before the end')
        if self.category not in CATEGORIES:
            raise ValueError('Category must be All, Run, Ride, Swim, or Other')

    @classmethod
    def parse(cls, start='', end='', category='All'):
        def parse_date(value, default):
            if not value:
                return default
            try:
                parsed = date.fromisoformat(value)
            except ValueError:
                raise ValueError('Use valid dates in YYYY-MM-DD format') from None
            if parsed.isoformat() != value:
                raise ValueError('Use valid dates in YYYY-MM-DD format')
            return parsed
        return cls(parse_date(start, SNAPSHOT_START), parse_date(end, SNAPSHOT_END), category)

    def utc_bounds(self):
        """Inclusive date controls become an explicit half-open UTC interval."""
        return (datetime.combine(self.start, time.min, timezone.utc),
                datetime.combine(self.end + timedelta(days=1), time.min, timezone.utc))

    def url(self, athlete_id, page=1):
        parameters = {'start': self.start.isoformat(), 'end': self.end.isoformat(),
                      'category': self.category}
        if page > 1:
            parameters['page'] = str(page)
        return f'/athlete/{athlete_id}?{urlencode(parameters)}#activities'


ACTIVITY_FILTER_CSS = """
.activity-section { container-type: inline-size; }
.activity-filters { display: grid; grid-template-columns: minmax(0,1fr); gap: .8rem; align-items: end; }
.activity-filter-field { min-width: 0; }
.activity-filters label { margin: 0; min-width: 0; font-size: 1rem; }
.activity-filters input, .activity-filters select { margin: .3rem 0 0; min-width: 0; font-size: 1rem; }
#activities .activity-filters input, #activities .activity-filters select { margin-bottom: 0; height: 3rem; }
.activity-filter-actions { display: flex; flex-wrap: wrap; align-items: center; gap: .8rem; }
.activity-filter-actions button { width: auto; margin: 0; padding: .3rem .6rem; min-height: 48px; font-size: 1rem; position: relative; }
.activity-filter-actions a { display: inline-flex; align-items: center; justify-content: center; min-height: 48px; min-width: 48px; }
.activity-filter-actions button:focus-visible { outline: 2px solid var(--primary); outline-offset: 2px; }
.activity-share { margin: 1rem 0; font-size: 1rem; }
.activity-section .pagination { flex-wrap: wrap; font-size: 1rem; }
.activity-section .pagination a { margin: 0; min-height: 48px; display: inline-flex; align-items: center; justify-content: center; }
.activity-section .table-wrap:focus-visible { outline: 2px solid var(--primary); outline-offset: 2px; }
.activity-section th { white-space: nowrap; }
.activity-section table { font-size: 1rem; min-width: 48rem; }
.activity-section th, .activity-section td, .activity-section td small { font-size: 1rem; }
.activity-section td:nth-child(2) { min-width: 24ch; max-width: 42ch; overflow-wrap: anywhere; }
.activity-section td:not(:nth-child(2)) { white-space: nowrap; }
.activity-table-inner { min-width: 100%; display: inline-block; vertical-align: middle; padding: .5rem 0; }
@media (pointer: coarse) {
  .activity-filter-actions .touch-target { position: absolute; top: 50%; left: 50%; width: max(100%,48px); height: max(100%,48px); transform: translate(-50%,-50%); }
}
@container (max-width: 599px) {
  .activity-section .pagination { display: grid; grid-template-columns: minmax(0,1fr) minmax(0,1fr); gap: .5rem; }
  .activity-section .pagination > span { display: none; }
  .activity-section .pagination a:first-child { grid-column: 1; }
  .activity-section .pagination a:last-child { grid-column: 2; }
}
@container (min-width: 850px) {
  .activity-filters { grid-template-columns: minmax(0,1fr) minmax(0,1fr) minmax(0,1fr) auto; }
  .activity-filters label, .activity-filters input, .activity-filters select, .activity-share, .activity-section .pagination { font-size: .875rem; }
  .activity-filter-actions button { min-height: 38px; font-size: .875rem; }
  .activity-section table, .activity-section th, .activity-section td, .activity-section td small { font-size: .875rem; }
}
"""


def activity_filter_form(athlete_id, filters):
    return (
        P('Filter the activity table only. The running charts above remain the full 2024 snapshot.', id='activity-filter-scope'),
        Form(
            Div(Label('From (UTC)', fr='activity-start'), Input(type='date', name='start', value=filters.start.isoformat(),
                  min='2024-01-01', max='2024-12-31', id='activity-start'), cls='activity-filter-field'),
            Div(Label('Through (UTC)', fr='activity-end'), Input(type='date', name='end', value=filters.end.isoformat(),
                  min='2024-01-01', max='2024-12-31', id='activity-end'), cls='activity-filter-field'),
            Div(Label('Activity category', fr='activity-category'), Select(*[Option('All activities' if c == 'All' else c,
                  value=c, selected=c == filters.category) for c in CATEGORIES],
                  name='category', id='activity-category'), cls='activity-filter-field'),
            Div(Button('Apply filters', Span(cls='touch-target', aria_hidden='true'), type='submit'),
                A('Reset', href=f'/athlete/{athlete_id}#activities'), cls='activity-filter-actions'),
            action=f'/athlete/{athlete_id}#activities', method='get', cls='activity-filters',
            aria_describedby='activity-filter-scope',
        ),
        P(A('Link to these filters', href=filters.url(athlete_id)),
          ' · Dates include both endpoints. “Other” retains each activity’s original type below.', cls='activity-share'),
    )
