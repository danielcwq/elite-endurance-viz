"""Shared visual language; existing map, search, and data boundaries remain intact."""
from fasthtml.common import A, Details, Div, Nav, Span, Summary

FONT_URL = 'https://rsms.me/inter/inter.css'
UI_CSS = """
html:root,html:root:not([data-theme]),html[data-theme] { --font-family: 'InterVariable', system-ui, sans-serif; --font-size: 16px;
 --background-color: #fafbf9; --color: #25312e; --muted-color: #64736e;
 --primary: #246b56; --primary-hover: #18523f; --primary-inverse: #fff;
 --ev-line: #52615b30; --ev-surface: #fff; --ev-soft: #eff3ef; --ev-ink: #17251f;
 --form-element-background-color: var(--ev-surface); --form-element-border-color: #7a8c8145;
 --form-element-color: var(--color); }
body { font-family: var(--font-family); font-optical-sizing:auto;
 font-feature-settings:'cv02','cv03','cv04','cv11'; -webkit-font-smoothing:antialiased; }
main { isolation: isolate; }
h1,h2,h3 { font-weight: 600; color: var(--ev-ink); text-wrap:balance; }
h1 { letter-spacing:-.045em; } p { text-wrap:pretty; }
.container { width: min(1200px, calc(100% - 64px)); max-width:1200px; padding-left:0; padding-right:0; }
.ev-nav { display:flex; align-items:center; justify-content:space-between; gap:2rem;
 border-bottom:1px solid var(--ev-line); padding:1.15rem 0; margin-bottom:2rem; }
.ev-logo { font-weight:600; letter-spacing:-.04em; font-size:1.3rem; }
.ev-nav a { text-decoration:none; color:var(--muted-color); padding:.5rem 0; }
.ev-nav .ev-logo,.ev-nav a[aria-current=page] { color:var(--ev-ink); }
.ev-nav-links { display:flex; gap:1.5rem; align-items:center; }
.ev-nav .ev-mobile-nav { display:none; margin:0; padding:0; border:0; }
.ev-kicker { font-family:ui-monospace,monospace; font-size:.8rem; letter-spacing:.09em; text-transform:uppercase; color:var(--muted-color); }
.muted { color:var(--muted-color); }
.review-note { border-left:3px solid #bb8546; padding:.5rem 1rem; font-size:1rem; max-width:90ch; }
.snapshot-banner { background:none; border:0; padding:0; border-radius:0;
 color:var(--muted-color); font-size:.8rem; margin:1rem 0; }
.snapshot-banner small { font-size:inherit; }
.home-page { padding-top:0; }
.home-page h1 { font-size:clamp(2.5rem,5.4vw,4.5rem); max-width:19ch; margin:1.3rem 0; line-height:1.08; }
.home-page .home-intro { max-width:58ch; font-size:1.1rem; margin-bottom:1.5rem; }
.home-actions { display:flex; gap:1.3rem; align-items:center; flex-wrap:wrap; margin:1.5rem 0 3rem; }
.home-actions a[role=button] { margin:0; padding:.6rem .9rem; width:auto;
 background:var(--primary); border-color:var(--primary); color:var(--primary-inverse); }
.home-page .explorer-controls { max-width:none; margin-bottom:1.4rem; }
.home-page h2 { margin:2rem 0 1rem; font-size:1.25rem; letter-spacing:-.025em; }
.home-page .metric-grid { gap:1.5rem; border-top:1px solid var(--ev-line); padding-top:1.3rem; }
.home-page .metric { border:0; background:none; border-radius:0; padding:0; min-width:0; }
.home-page .metric strong { font-size:2rem; font-weight:500; letter-spacing:-.04em; font-variant-numeric:tabular-nums; }
.home-page .metric span { font-size:.85rem; color:var(--muted-color); }
.home-note { font-size:.9rem; }
.ev-table-scroll { overflow:auto; margin:.5rem 0; }
.ev-table-scroll table { width:100%; }
.ev-table-scroll th { white-space:nowrap; }
.ev-table-scroll th,.ev-table-scroll td { font-size:.85rem; border-color:var(--ev-line); padding:.8rem .6rem; }
.ev-table-scroll td:first-child { min-width:20ch; }
.ev-table-scroll:focus-visible,:is(button,a,summary,select):focus-visible { outline:2px solid var(--primary); outline-offset:2px; }
@media(max-width:700px) {
 .container { width:calc(100% - 32px); }
 .ev-nav { gap:1rem; margin-bottom:1.4rem; }
 .ev-nav-links { display:none; }
 .ev-nav .ev-mobile-nav { display:block; }
 .ev-mobile-nav summary { padding:.7rem; min-height:48px; }
 .ev-mobile-nav[open] .ev-mobile-links { position:absolute; right:16px; top:70px; z-index:20;
 background:var(--ev-surface); border:1px solid var(--ev-line); padding:1rem; min-width:190px; }
 .ev-mobile-links a { display:block; min-height:48px; }
 .ev-kicker,.snapshot-banner,.ev-table-scroll th,.ev-table-scroll td,.home-note,.home-page .metric span { font-size:1rem; }
 .home-actions a[role=button] { min-height:48px; }
}
@media(prefers-color-scheme:dark) {
 html:root:not([data-theme=light]),html[data-theme=dark] { --background-color:#111715; --color:#d8e0dc; --muted-color:#9eaea5;
 --primary:#9ed7ba; --primary-hover:#c5ead7; --primary-inverse:#10291f;
 --ev-line:#b4cdb52a; --ev-surface:#18201c; --ev-soft:#1b2821; --ev-ink:#eef3ed;
 --form-element-background-color:#18201c; --form-element-border-color:#8da49350; }
 .snapshot-banner { background:none; border:0; }
 .home-page .metric { background:none; }
 .search-results { background:var(--ev-surface); }
}
"""


def navigation(active=''):
    def links():
        return [A(label,href=url,aria_current='page' if active==key else None)
                for key,label,url in [('home','Overview','/'),('compare','Compare','/compare'),('recordings','Recorded athletes','/recordings')]]
    return Nav(A('EnduranceViz',href='/',aria_label='Homepage',cls='ev-logo'),
               Div(*links(),cls='ev-nav-links'),
               Details(Summary('☰ Menu'),Div(*links(),cls='ev-mobile-links'),cls='ev-mobile-nav'),
               cls='ev-nav',aria_label='Main navigation')


EVENT_EXPLANATION = (
    'Assigned event is a grouping rule, not a declared specialty: the event with the highest stored 2024 '
    'World Athletics result points wins. Ties use the number of stored results, then the event name '
    'alphabetically. Missing scores sort last. An athlete can race other events; this label does not '
    'say which event they trained for. The source’s 1,100-point floor and incomplete result collection '
    'can affect the assignment.'
)
