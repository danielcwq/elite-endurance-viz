"""Shared-axis cohort overlays; training and score definitions remain unchanged."""
from html import escape
from math import ceil
from statistics import median
from fasthtml.common import (A, Button, Details, Div, Form, H1, H2, Label, Main,
    Option, P, Select, Style, Summary, Table, Tbody, Td, Th, Thead, Tr, NotStr,
    Span, Strong, Script)
from enduranceviz.recorded_training import PREVIEW_EVENTS
from enduranceviz.page_metadata import metadata, is_synthetic
from enduranceviz.ui import navigation, EVENT_EXPLANATION
from enduranceviz.review_notes import IDENTITY_REVIEW_NOTES

METRICS = {
    'distance': ('Running distance', 'median_recorded_week_run_distance_meters', 'distance_measured_weeks', 1000),
    'records': ('Run-record count', 'median_recorded_week_run_count', 'recorded_run_weeks', 1),
}
SEXES = {'both': ('female','male'), 'female': ('female',), 'male': ('male',)}
SEX_LABELS = {'female':'Women','male':'Men'}
CSS = """
.comparison-page { padding-top:0; font-variant-numeric:tabular-nums; }
.comparison-page h1 { font-size:clamp(2rem,4vw,3.3rem); margin:.4rem 0 .75rem; }
.comparison-page .intro { max-width:72ch; color:var(--muted-color); margin-bottom:1.5rem; }
.compare-controls-wrap,.overlay-layout-wrap { container-type:inline-size; }
.comparison-controls { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:1rem;
 padding:1.2rem 0; margin:0; border-top:1px solid var(--ev-line); border-bottom:1px solid var(--ev-line); align-items:end; }
.comparison-controls>* { min-width:0; }
.comparison-controls label { color:var(--muted-color); font-size:1rem; margin-bottom:.35rem; }
.comparison-controls select,.comparison-controls button { font-size:1rem; margin:0; padding:.5rem .7rem; min-height:48px; }
.comparison-controls button { width:auto; }
.comparison-controls button[type=submit] { background:var(--primary); border-color:var(--primary); color:var(--primary-inverse); }
.comparison-page h2 { font-size:1.3rem; letter-spacing:-.025em; margin:0; }
.chart-heading { display:flex; align-items:baseline; justify-content:space-between; gap:1rem; flex-wrap:wrap; padding:1.6rem 0 1rem; }
.chart-heading p { margin:0; font-size:.85rem; color:var(--muted-color); }
.overlay-layout { display:grid; grid-template-columns:minmax(0,1fr); gap:1.5rem; }
.overlay-plot { min-width:0; background:var(--ev-surface); border:1px solid var(--ev-line); border-radius:8px; padding:1rem; }
.comparison-scroll { overflow:auto; }
.comparison-chart { display:block; width:100%; min-width:660px; color:var(--muted-color); }
.comparison-chart text { fill:currentColor; font-size:22px; }
.comparison-chart .grid { stroke:var(--ev-line); stroke-width:1; }
.s0 { --series:#207d6a; } .s1 { --series:#385dcc; } .s2 { --series:#b96424; } .s3 { --series:#a84480; }
.series { color:var(--series); }
.curve { stroke:currentColor; fill:none; stroke-width:3; pointer-events:none; }
.curve-hit { stroke:transparent; stroke-width:16; fill:none; pointer-events:none; }
.dot { fill:currentColor; stroke:var(--ev-surface); stroke-width:1.4; cursor:pointer; }
.male .curve { stroke-dasharray:9 5; }
.male .dot { fill:var(--ev-surface); stroke:currentColor; stroke-width:2; }
.dot[data-active=true] { stroke:var(--ev-ink); stroke-width:3; r:8px; }
.compare-root[data-focus] .series { opacity:.12; }
.compare-root[data-focus="0"] .series[data-series="0"],.compare-root[data-focus="1"] .series[data-series="1"],
.compare-root[data-focus="2"] .series[data-series="2"],.compare-root[data-focus="3"] .series[data-series="3"] { opacity:1; }
.cohort-key { display:grid; gap:.6rem; align-content:start; }
.compare-root .cohort-button { text-align:left; width:100%; margin:0; border:1px solid transparent; background:transparent;
 color:var(--ev-ink); padding:.8rem; border-radius:6px; font-size:.9rem; box-shadow:none; }
.compare-root .cohort-button:hover,.compare-root .cohort-button[aria-pressed=true] { border-color:var(--ev-line); background:var(--ev-soft); }
.cohort-name { font-weight:600; display:flex; gap:.65rem; align-items:center; }
.swatch { width:1.5rem; height:0; border-top:3px solid var(--series); flex-shrink:0; }
.male .swatch { border-top-style:dashed; }
.cohort-value { font-size:1.5rem; color:var(--ev-ink); margin:.25rem 0; letter-spacing:-.025em; }
.cohort-note { color:var(--muted-color); font-size:.8rem; }
.chart-caption,.inspection { color:var(--muted-color); font-size:.85rem; margin:.8rem 0 0; }
.inspection { border-top:1px solid var(--ev-line); padding-top:1rem; min-height:105px; }
.inspection p { margin:0 0 .4rem; }
.inspection strong { color:var(--ev-ink); }
.compare-root .reset-focus { color:var(--muted-color); border:0; background:transparent; box-shadow:none;
 font-size:.85rem; padding:.4rem .7rem; width:auto; margin:.3rem 0; min-height:32px; }
.comparison-page details:not(.ev-mobile-nav) { margin:1.5rem 0; border-top:1px solid var(--ev-line); padding-top:1rem; }
.comparison-page details:not(.ev-mobile-nav) summary { min-height:48px; color:var(--color); }
.comparison-page details:not(.ev-mobile-nav) p { max-width:90ch; font-size:.9rem; }
.compare-root .value-inspect { background:none; border:0; color:var(--primary); margin:0; padding:.25rem;
 font-size:inherit; text-align:left; width:auto; box-shadow:none; }
.comparison-page [hidden] { display:none!important; }
@container(min-width:1000px) {
 .comparison-controls { grid-template-columns:1fr 1fr 1fr 1.1fr 1.3fr auto; }
 .inventory-controls { grid-template-columns:2fr 1.5fr 2fr auto; }
 .comparison-controls select,.comparison-controls button { font-size:.85rem; min-height:38px; height:38px; }
 .comparison-controls label { font-size:.8rem; }
 .overlay-layout { grid-template-columns:minmax(0,1fr) 260px; }
}
@media(max-width:700px) {
 .comparison-controls>div:nth-child(4),.comparison-controls>div:nth-child(5) { grid-column:1/-1; }
 .overlay-plot { padding:.6rem; }
 .comparison-page .intro,.chart-caption,.inspection,.cohort-note,.chart-heading p,.comparison-page details p { font-size:1rem; }
 .cohort-key { grid-template-columns:repeat(2,minmax(0,1fr)); }
 .cohort-button { padding:.7rem .3rem; font-size:1rem; }
 .cohort-name { align-items:start; }
 .reset-focus,.value-inspect { min-height:48px; font-size:1rem; }
}
@media(prefers-color-scheme:dark) {
 .s0 { --series:#83d6b2; } .s1 { --series:#9cafff; } .s2 { --series:#eeb37a; } .s3 { --series:#e699c5; }
}
"""
INTERACTION_JS = """
(() => {
 const root=document.querySelector('.compare-root'); if(!root || root.dataset.bound) return;
 root.dataset.bound='true'; let pinned=null;
 const heading=root.querySelector('#inspect-name'), info=root.querySelector('#inspect-info');
 const link=root.querySelector('#inspect-link'), keys=[...root.querySelectorAll('.cohort-button')];
 const focus=id=>{if(id===null) root.removeAttribute('data-focus'); else root.dataset.focus=id;};
 const clearPoint=()=>root.querySelectorAll('[data-active]').forEach(e=>e.removeAttribute('data-active'));
 const resetInfo=()=>{heading.textContent='Inspect an athlete';info.textContent='Hover a dot, or tap it to pin its details. Use the values table for keyboard selection.';link.hidden=true;clearPoint();};
 const inspect=el=>{
   clearPoint(); const marker=root.querySelector('.dot[data-point="'+el.dataset.point+'"]');
   if(marker) marker.dataset.active='true';
   focus(el.dataset.series);heading.textContent=el.dataset.name;info.textContent=el.dataset.info;
   link.href=el.dataset.url;link.hidden=false;
 };
 root.addEventListener('pointerover',event=>{
   if(pinned!==null)return;
   const point=event.target.closest('[data-point]');if(point){inspect(point);return;}
   const group=event.target.closest('[data-series]');if(group)focus(group.dataset.series);
 });
 root.addEventListener('pointerout',event=>{
   if(pinned!==null)return;
   const next=event.relatedTarget;
   if(!next || !root.contains(next) || !next.closest('[data-series]')){focus(null);resetInfo();}
 });
 root.addEventListener('focusin',event=>{
   if(pinned!==null)return;
   const point=event.target.closest('[data-point]');
   if(point)inspect(point);else if(event.target.closest('.cohort-button'))focus(event.target.dataset.series);
 });
 root.addEventListener('focusout',event=>{
   if(pinned===null && (!event.relatedTarget || !event.relatedTarget.closest('[data-series]'))){focus(null);resetInfo();}
 });
 root.addEventListener('click',event=>{
   const point=event.target.closest('[data-point]');
   if(point){pinned=point.dataset.series;inspect(point);keys.forEach(k=>k.setAttribute('aria-pressed','false'));return;}
   const key=event.target.closest('.cohort-button');
   if(key){pinned=pinned===key.dataset.series?null:key.dataset.series;focus(pinned);resetInfo();
     keys.forEach(k=>k.setAttribute('aria-pressed',String(k.dataset.series===pinned)));}
 });
 const reset=()=>{pinned=null;focus(null);resetInfo();keys.forEach(k=>k.setAttribute('aria-pressed','false'));};
 root.querySelector('.reset-focus').addEventListener('click',reset);
 root.addEventListener('keydown',event=>{if(event.key==='Escape')reset();});
})();
"""

def quantile(values, fraction):
    ordered=sorted(values); position=(len(ordered)-1)*fraction; low=int(position)
    return ordered[low]+(ordered[min(low+1,len(ordered)-1)]-ordered[low])*(position-low)

def metric_rows(rows, metric, view):
    _,field,weeks,divisor=METRICS[metric]
    return [dict(r,value=float(r[field])/divisor,weeks=r[weeks]) for r in rows
            if r[field] is not None and (view!='points' or r['best_stored_results_score'] is not None)]

def point_info(row, label, unit):
    score=row['best_stored_results_score']
    return (f"{label} · {row['value']:.1f} {unit}/recorded week · {row['weeks']}/52 contributing weeks · "
            f"{row['source_warning_run_weeks']} source-warning Run weeks · "
            f"{score if score is not None else 'Unavailable'} points. "
            f"Stored event results: {row.get('event_results') or 'Unavailable'}. "
            + IDENTITY_REVIEW_NOTES.get(row['athlete_id'],''))

def overlay_chart(groups, view, unit):
    left,right,top,bottom=72,870,48,360
    rows=[r for g in groups for r in g['rows']]
    maximum=max(1,max((r['value'] for r in rows),default=1))
    step=25 if unit=='km' else 5
    maximum=max(step,ceil(maximum/step)*step)
    scores=[r['best_stored_results_score'] for r in rows if r['best_stored_results_score'] is not None]
    low,high=(min(scores)-10,max(scores)+10) if scores else (1100,1200)
    parts=['<svg class="comparison-chart" viewBox="0 0 910 430" role="img" aria-labelledby="overlay-title overlay-description">',
           '<title id="overlay-title">Overlaid cohort comparison</title>',
           '<desc id="overlay-description">Each colour is an event and recorded-sex group, never a pooled cohort. Exact values and recording breadth are in the values table.</desc>']
    def text(x,y,value,anchor='middle'):
        return f'<text x="{x}" y="{y}" text-anchor="{anchor}">{escape(str(value))}</text>'
    for i in range(5):
        x=left+(right-left)*i/4;y=bottom-(bottom-top)*i/4
        parts += [f'<path class="grid" d="M{left} {y} H{right} M{x} {top} V{bottom}"/>',
                  text(left-12,y+6,f'{i*25}%' if view=='distribution' else f'{maximum*i/4:g}','end'),
                  text(x,390,f'{maximum*i/4:g}' if view=='distribution' else f'{low+(high-low)*i/4:.0f}')]
    parts.append(text(left,25,'Share of athletes at or below this value' if view=='distribution' else f'Median {unit} / recorded week','start'))
    parts.append(text(470,423,f'Median {unit} / recorded week' if view=='distribution' else 'Highest stored assigned-event result points'))
    for group in groups:
        gid=group['id']; ordered=sorted(group['rows'],key=lambda r:(r['value'],r['athlete_id']))
        parts.append(f'<g class="series s{gid} {group["sex"]}" data-series="{gid}">')
        if view=='distribution' and ordered:
            path=f'M{left} {bottom}'
            for i,row in enumerate(ordered):
                x=left+row['value']/maximum*(right-left);y=bottom-(i+1)/len(ordered)*(bottom-top)
                path+=f' H{x:.3f} V{y:.3f}'
            path+=f' H{right}'
            parts += [f'<path class="curve" d="{path}"/>',f'<path class="curve-hit" d="{path}"/>']
        for i,row in enumerate(ordered):
            if view=='distribution':
                x=left+row['value']/maximum*(right-left)
                y=bottom-sum(r['value']<=row['value'] for r in ordered)/len(ordered)*(bottom-top)
            else:
                x=left+(row['best_stored_results_score']-low)/(high-low)*(right-left)
                y=bottom-row['value']/maximum*(bottom-top)
            attrs={'data-point':row['athlete_id'],'data-series':gid,'data-name':row['name'],
                   'data-info':point_info(row,group['label'],unit),'data-url':f"/athlete/{row['athlete_id']}"}
            encoded=' '.join(f'{k}="{escape(str(v),quote=True)}"' for k,v in attrs.items())
            parts.append(f'<circle class="dot" cx="{x:.3f}" cy="{y:.3f}" r="4.5" {encoded}><title>{escape(row["name"])} · {escape(attrs["data-info"])}</title></circle>')
        parts.append('</g>')
    if not rows: parts.append(text(470,210,'No contributing observations'))
    parts.append('</svg>')
    return NotStr(''.join(parts))

def select_control(name,title,options,selected):
    return Div(Label(title,fr=f'compare-{name}'),
        Select(*(Option(label,value=value,selected=value==selected) for value,label in options),name=name,id=f'compare-{name}'))

def comparison_page(repository,banner,first='800m',second='5000m',metric='distance',view='distribution',sex='both'):
    if first not in PREVIEW_EVENTS or second not in PREVIEW_EVENTS or metric not in METRICS or view not in ('distribution','points') or sex not in SEXES:
        raise ValueError('Choose a listed event, metric, view, and recorded-sex option.')
    events=list(dict.fromkeys((first,second)))
    source=[r for r in repository.comparison_athletes() if r['primary_discipline'] in events]
    groups=[]; unit='km' if metric=='distance' else 'Run records'
    for e,event in enumerate(events):
        for s in SEXES[sex]:
            inventory=[r for r in source if r['primary_discipline']==event and r['gender']==s]
            groups.append(dict(id=e*2+int(s=='male'),sex=s,label=f'{event} · {SEX_LABELS[s]}',
                               inventory=inventory,rows=metric_rows(inventory,metric,view)))
    keys=[]; tables=[]
    for g in groups:
        rows=g['rows']; n=len(rows); values=[r['value'] for r in rows]
        mid=f'{median(values):.1f}' if n else '—'
        weeks=f'{median(r["weeks"] for r in rows):g}' if n else '—'
        keys.append(Button(Div(Span(cls='swatch',aria_hidden='true'),g['label'],cls='cohort-name'),
            Div('Group median',cls='cohort-note'),
            Div(mid,Span(f' {unit}/wk'),cls='cohort-value'),
            Div(f'{n} contributors · median {weeks}/52 weeks',cls='cohort-note'),
            type='button',cls=f'cohort-button s{g["id"]} {g["sex"]}',**{'data-series':str(g['id'])},aria_pressed='false',
            aria_label=f'Highlight {g["label"]}'))
        for r in rows:
            tables.append(Tr(Td(Button(r['name'],type='button',cls='value-inspect',
                 **{'data-point':r['athlete_id'],'data-series':str(g['id']),'data-name':r['name'],
                    'data-info':point_info(r,g['label'],unit),'data-url':f"/athlete/{r['athlete_id']}"})),
                 Td(g['label']),Td(f"{r['value']:.2f}"),Td(f"{r['weeks']}/52"),Td(r['recorded_run_weeks']),
                 Td(r['source_warning_run_weeks']),Td(r['best_stored_results_score'] if r['best_stored_results_score'] is not None else 'Unavailable'),
                 Td(A('Profile',href=f"/athlete/{r['athlete_id']}"))))
    def counts_row(g):
        rows=g['rows']; values=[r['value'] for r in rows]
        return Tr(Td(g['label']),Td(len(g['inventory'])),Td(len(rows)),Td(len(g['inventory'])-len(rows)),
                  Td(f'{quantile(values,.25):.1f}–{quantile(values,.75):.1f}' if values else 'Unavailable'),
                  Td(sum(r['source_warning_run_weeks']>0 for r in rows)))
    stats=repository.snapshot_stats(); synthetic=is_synthetic(stats)
    flagged=sum(r['athlete_id'] in IDENTITY_REVIEW_NOTES for g in groups for r in g['rows'])
    return (*metadata('Compare recorded running — EnduranceViz 2024'+(' · Synthetic Demo' if synthetic else ''),
            'Invented software-test data.' if synthetic else 'Overlaid event and recorded-sex comparisons with athlete-level values and recording breadth.',synthetic),
        Style(CSS),Main(navigation('compare'),P('2024 research explorer',cls='ev-kicker'),
        H1('See the differences. Keep the context.'),
        P('Compare recorded running across events, or inspect how it relates to performance. Each athlete contributes one weekly median—not an estimated training year.',cls='intro'),
        Div(Form(select_control('first','Assigned event A',[(e,e) for e in PREVIEW_EVENTS],first),
            select_control('second','Assigned event B',[(e,e) for e in PREVIEW_EVENTS],second),
            select_control('sex','Recorded sex',[('both','Women + men'),('female','Women'),('male','Men')],sex),
            select_control('metric','Weekly metric',[(k,v[0]) for k,v in METRICS.items()],metric),
            select_control('view','Chart',[('distribution','Distribution overlay'),('points','Performance points')],view),
            Button('Update',type='submit'),action='/compare',method='get',cls='comparison-controls'),cls='compare-controls-wrap'),
        Div(Div(H2('Recorded-week distributions' if view=='distribution' else 'Performance and recorded running'),
                P('Colours identify cohorts · hover to highlight · click to pin'),cls='chart-heading'),
            Div(Div(Div(overlay_chart(groups,view,unit),cls='comparison-scroll',tabindex='0',role='region',aria_label='Overlaid cohort chart'),
                    P('Read across at 50% to compare the centres. Curves show the share of athletes at or below each value; no smoothing or pooling.'
                      if view=='distribution' else 'Each dot is one athlete. Event/sex series stay separate; there is no fitted line or pooled score.',cls='chart-caption'),
                    Div(P(Strong('Inspect an athlete',id='inspect-name')),P('Hover a dot, or tap it to pin its details. Use the values table for keyboard selection.',id='inspect-info'),
                        A('Open athlete profile →',href='#',id='inspect-link',hidden=True),cls='inspection',aria_live='polite'),cls='overlay-plot'),
                Div(*keys,Button('Reset highlight',type='button',cls='reset-focus'),cls='cohort-key'),cls='overlay-layout'),cls='overlay-layout-wrap'),
        Details(Summary('What does “assigned event” mean?'),P(EVENT_EXPLANATION),
            P('Open an athlete’s profile to see their other stored events and the points behind the assignment. Marathon is now available in this preview; it was outside the original six-event static exploration.')),
        P(f'{flagged} plotted identity link is under review. The row is retained; inspect its point or profile for details.',cls='review-note') if flagged else None,
        Details(Summary('Cohort counts, missing values, and middle 50%'),
            Div(Table(Thead(Tr(*(Th(x,scope='col') for x in ('Cohort','Registry','Contributors','Unavailable','Middle 50%','With source warnings')))),
                      Tbody(*(counts_row(g) for g in groups))),cls='ev-table-scroll',tabindex='0',role='region',aria_label='Cohort denominators'),
            P('Unavailable means no metric'+(' or no assigned-event score.' if view=='points' else '.')+' Middle 50% is the interquartile range of athlete medians, not a confidence interval. No annual-week cutoff or P0 eligibility filter.'),
            P(f"Other/unknown recorded-sex registry athletes in these events: {sum(r['gender'] not in ('female','male') for r in source)}; not pooled into these series.")),
        Details(Summary('Inspect all plotted athlete values'),
            Div(Table(Thead(Tr(*(Th(x,scope='col') for x in ('Athlete','Assigned cohort',f'{unit}/wk','Contributing weeks','Run weeks','Warning Run weeks','Points','Details')))),Tbody(*tables)),
                cls='ev-table-scroll',tabindex='0',role='region',aria_label='Plotted athlete values')),
        Details(Summary('Metric definitions and limitations'),
            P('Medians use full Monday–Sunday UTC weeks with recorded Runs. There are 52 full weeks in the 2024 window; December 30–31 is excluded. Distance requires all Run distances in the week to be present, finite, and nonnegative. Run counts are stored records, not independent sessions.'),
            P('Selective public posting and collection gaps remain. Missing weeks are not zero training. Contributing weeks measure recording breadth, not confidence. Warning counts cover all recorded-running weeks, not only distance-measured weeks.'),
            P('Points are the highest stored 2024 result score in the assigned event. Results in other events never substitute for missing scores. Overlays compare distinct series visually; they do not pool sexes or events, establish causation, or prove complete training histories.')),
        banner(stats),Script(INTERACTION_JS),cls='container comparison-page compare-root'))
