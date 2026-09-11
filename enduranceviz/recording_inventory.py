"""Searchable inventory of all accounts with stored activities, not all public workouts."""
from math import ceil
from urllib.parse import urlencode
from fasthtml.common import A, Button, Details, Div, Form, H1, Input, Label, Main, Option, P, Select, Summary, Table, Tbody, Td, Th, Thead, Tr
from enduranceviz.ui import navigation, EVENT_EXPLANATION
from enduranceviz.page_metadata import metadata, is_synthetic
from enduranceviz.comparison_preview import CSS
from fasthtml.common import Style


def inventory_page(repository,banner,q='',event='All',sort='weeks',page='1'):
    all_rows=[r for r in repository.recording_summaries() if r['recorded_activities_2024']>0]
    events=sorted({r['primary_discipline'] for r in all_rows if r['primary_discipline']})
    if event not in ['All','Unassigned',*events] or sort not in ('weeks','distance','name') or len(q)>200:
        raise ValueError('Choose a listed event and sort option; search is limited to 200 characters.')
    try: number=int(page)
    except ValueError: raise ValueError('Page must be a positive integer.')
    if number<1: raise ValueError('Page must be a positive integer.')
    rows=[r for r in all_rows if q.casefold().strip() in r['name'].casefold()
          and (event=='All' or (r['primary_discipline'] or 'Unassigned')==event)]
    if sort=='name': rows.sort(key=lambda r:(r['name'].casefold(),r['athlete_id']))
    elif sort=='distance': rows.sort(key=lambda r:(-(r['median_recorded_week_run_distance_meters'] if r['median_recorded_week_run_distance_meters'] is not None else -1),r['name']))
    else: rows.sort(key=lambda r:(-r['recorded_run_weeks'],-r['distance_measured_weeks'],r['name']))
    pages=max(1,ceil(len(rows)/50));number=min(number,pages)
    current=rows[(number-1)*50:number*50]
    def url(p): return '/recordings?'+urlencode(dict(q=q,event=event,sort=sort,page=p))
    stats=repository.snapshot_stats();synthetic=is_synthetic(stats)
    return (*metadata('Recorded athletes — EnduranceViz 2024'+(' · Synthetic Demo' if synthetic else ''),
        'Invented test records.' if synthetic else 'Every athlete with activities stored in this snapshot. Recording breadth is not completeness.',synthetic),
        Style(CSS),Main(navigation('recordings'),P('The observation inventory',cls='ev-kicker'),
        H1('Who has recorded activity?'),
        P(f"{len(all_rows):,} athletes have stored activities; {sum(r['recorded_runs_2024']>0 for r in all_rows):,} have Runs. "
          'This is the full inventory in our 2024 snapshot—not a census of everyone posting publicly on Strava.',cls='intro'),
        Div(Form(Div(Label('Find an athlete',fr='inventory-q'),Input(name='q',id='inventory-q',value=q,type='search')),
            Div(Label('Assigned event',fr='inventory-event'),Select(*(Option(e,value=e,selected=event==e) for e in ['All','Unassigned',*events]),name='event',id='inventory-event')),
            Div(Label('Order by',fr='inventory-sort'),Select(*(Option(label,value=value,selected=value==sort) for value,label in [('weeks','Most recorded weeks'),('distance','Highest recorded-week distance'),('name','Athlete name')]),name='sort',id='inventory-sort')),
            Button('Update',type='submit'),method='get',action='/recordings',cls='comparison-controls inventory-controls'),cls='compare-controls-wrap'),
        P(f'{len(rows):,} matching athletes · page {number} of {pages}. Sorted for inspection, not a completeness or training-quality ranking.'),
        Div(Table(Thead(Tr(*(Th(x,scope='col') for x in ('Athlete','Assigned event','Recorded sex','Run weeks /52','Distance weeks /52','Median km /week','Run records','Warning Run weeks')))),
            Tbody(*(Tr(Td(A(r['name'],href=f"/athlete/{r['athlete_id']}")),Td(r['primary_discipline'] or 'Unassigned'),Td(r['gender']),
                Td(r['recorded_run_weeks']),Td(r['distance_measured_weeks']),
                Td(f"{r['median_recorded_week_run_distance_meters']/1000:.1f}" if r['median_recorded_week_run_distance_meters'] is not None else 'Unavailable'),
                Td(r['recorded_runs_2024']),Td(r['source_warning_run_weeks'])) for r in current))),
            cls='ev-table-scroll',tabindex='0',role='region',aria_label='Recorded athlete inventory'),
        P(A('Previous',href=url(number-1)) if number>1 else None,' · ' if 1<number<pages else '',A('Next',href=url(number+1)) if number<pages else None),
        P('No matching athletes.' if not rows else 'Run records include partial December 30–31; weekly medians exclude it. Zero Run weeks here can mean the athlete has only non-running activity.'),
        Details(Summary('What counts as publicly recorded?'),
            P('At least one activity stored in this snapshot. We cannot confirm current account visibility, complete capture, or that every training session was posted. Activity names and descriptions can suggest workouts, but the database has no validated structured-workout label.'),
            P('Contributing-week counts and source warnings are separate: activity evidence can remain in a week whose collection summary was inconsistent. Neither a year of posts nor a high record count establishes complete training.'),P(EVENT_EXPLANATION)),
        banner(stats),cls='container comparison-page'))
