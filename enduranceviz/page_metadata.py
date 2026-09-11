"""Factual page/share metadata; no host-derived URLs or inferred training claims."""

from fasthtml.common import Meta, Title

from enduranceviz.training_profile import summarize_weeks


def is_synthetic(stats):
    return str(stats['dataset_version']).startswith('SYNTHETIC-DEMO')


def metadata(title, description, synthetic=False):
    tags = [Title(title), Meta(name='description',content=description),
            Meta(property='og:title',content=title), Meta(property='og:description',content=description),
            Meta(property='og:type',content='website'), Meta(property='og:site_name',content='EnduranceViz'),
            Meta(name='twitter:card',content='summary'), Meta(name='twitter:title',content=title),
            Meta(name='twitter:description',content=description)]
    if synthetic:
        tags.append(Meta(name='robots',content='noindex, nofollow'))
    return tags


def homepage_metadata(stats):
    synthetic = is_synthetic(stats)
    title = 'EnduranceViz — Synthetic Demo' if synthetic else 'EnduranceViz — 2024 Snapshot'
    description = ('Invented athletes, results, and activities for software testing. Not real training data.'
                   if synthetic else 'Explore athletes, performances, and publicly recorded activity in the 2024 snapshot. '
                   'Public records are not a complete or current training history.')
    return metadata(title,description,synthetic)


def profile_metadata(athlete, stats, weeks):
    name = athlete['display_name'] or athlete['official_name']
    synthetic = is_synthetic(stats)
    title = f'{name} — EnduranceViz 2024' + (' · Synthetic Demo' if synthetic else '')
    if synthetic:
        description = f'{name}: invented software-test data, not a real athlete or training history.'
    else:
        summary = summarize_weeks(weeks)
        event = athlete['primary_discipline'] or 'athlete profile'
        description = (f"{name} · {event} · 2024 snapshot. {summary['run_weeks']} of "
                       f"{summary['full_weeks']} full UTC weeks contain recorded runs. "
                       'Public records, not a complete training history.')
        if summary['warning_run_weeks']:
            description += f" {summary['warning_run_weeks']} recorded-running weeks have source warnings."
    return metadata(title,description,synthetic)
