"""Diagnostic classification of legacy weekly disagreements; never selects a winner."""

import re
from decimal import Decimal, InvalidOperation

import pandas as pd

FIELDS = ('Date Range', 'Distance (km)', 'Time', 'Elevation (m)')


def text_value(value):
    return '' if value is None or pd.isna(value) else str(value).strip()


def numeric_value(value):
    text = text_value(value)
    if not text:
        return ('missing',)
    try:
        return ('number', Decimal(text))
    except InvalidOperation:
        return ('text', text)


def time_value(value):
    text = text_value(value)
    match = re.fullmatch(r'(?:(\d+)h\s*)?(?:(\d+)m\s*)?(?:(\d+)s)?', text)
    if match and any(part is not None for part in match.groups()):
        return ('seconds', sum(int(part or 0) * scale for part, scale in zip(match.groups(), (3600, 60, 1))))
    return ('text', text)


def classify_conflict(records):
    """Exclusive diagnostic categories. Numeric equivalence uses no tolerance."""
    raw = {tuple(text_value(row.get(field)) for field in FIELDS) for row in records}
    if len(raw) <= 1:
        return 'identical'
    ranges = {text_value(row.get('Date Range')) for row in records}
    no_data = {bool(re.fullmatch(r'Week \d+ - No Data', value)) for value in ranges}
    if no_data == {True, False}:
        return 'no_data_vs_summary'
    if len(ranges) > 1:
        return 'different_date_range_text'
    normalized = {
        (numeric_value(row.get('Distance (km)')), time_value(row.get('Time')),
         numeric_value(row.get('Elevation (m)')))
        for row in records
    }
    return 'different_totals_or_missing_fields' if len(normalized) > 1 else 'equivalent_numeric_or_time_format'
