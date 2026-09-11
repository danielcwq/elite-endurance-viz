# UI and recording review — 2026-09-10

P1 preview only. No production deployment, packaged database rewrite, identity
correction, analytical cutoff, or completed editorial case study is implied.

## What changed

The homepage, navigation and comparison use Inter, a quieter green/neutral palette,
clearer hierarchy and responsive controls. Search and the nationality map remain.
The comparison puts selected event/sex groups on one chart with hover/focus
highlighting, click-to-pin details, and women/men/both selection. The distribution
view is an ECDF of the existing per-athlete medians; performance points remain
continuous. Groups share axes, not pooled calculations. Exact values and coverage
remain available in the keyboard-accessible table and point inspector.

Run the app and open `/compare` or `/recordings`. The review session is on port
8011. See [preview usage and checks](p1-comparison-preview.md).

## Assigned event, not a verified main specialty

Existing rule: group stored 2024 results by athlete/event; take each event's best
World Athletics result points. Choose the highest score, breaking ties by number
of stored results, then event name alphabetically. Missing points sort last.
The 1,100-point source floor and incomplete result collection limit this proxy.
There are 1,110 athletes with more than one distinct stored event.

Samuel Barata illustrates the ambiguity: 10000m has 1,149 points, half marathon
1,107, and marathon 1,105. The rule assigns 10000m; it does not establish that he
only races or trains for that event. Profiles and comparison details now show
the stored event/score portfolio. No stored assignments were changed.

Marathon was omitted from the original six-event comparison, not from the data.
There are 1,115 marathon-assigned registry athletes (441 women / 674 men); 89 have
recorded-running-week metrics (29 / 60). Marathon is now in the interactive
preview. The existing six-event static figures remain unchanged.

## Proposed anomaly questions

Distances below are each athlete's median over distance-measured full weeks with
stored Runs, not annual averages or estimates of complete training. These are
purposive inspection examples, not a ranked or representative sample. An initial
40-distance-week screen helped locate broadly recorded high/low examples; it is
not an eligibility cutoff and is not applied to the comparison or full inventory.

| Athlete | Assigned event | Median km/week | Distance weeks | Question worth investigating |
| --- | --- | ---: | ---: | --- |
| Erik Hille | Marathon | 179.89 | 51 | What do high-volume recorded blocks look like? 595 annual Run records, one warning Run week. |
| Romain Legendre | 5000m | 151.47 | 49 | How does a high-volume 5000m record compare with marathon-assigned accounts? 546 annual Run records, two warning weeks. |
| Caden Norris | 800m | 24.14 | 47 | Why 11 median Run records/week but low recorded distance? Inspect short-record granularity before treating records as sessions. |
| Sarah McDonald | 1500m | 9.73 | 44 | Broad calendar coverage but median one Run record/week: what portion of running is actually represented? |
| Rebecca Bassett | 5000m | 15.58 | 49 | Another broad-but-low-volume record, with median three Run records/week; compare recording density before training interpretation. |
| Laura Nagel | 1500m | 102.73 | 52 | Dense year-long activity evidence but 45 warning Run weeks: how do collection-summary problems coexist with actual records? |
| Samuel Barata | 10000m | 166.45 | 45 | Cross-event athlete: how sensitive is the event story to assigning him to his highest-point result? |

### Identity review before a training story

The existing Kate Mitchell linkage needs independent validation. The registry
contains an 800m result of **2:01.13** (1,142 points, 2024-06-08, Concord, MA).
The linked Strava account **49462685** has an activity description mentioning a
5 km time of **32:12** and a goal below 30 minutes. That is a possible same-name
account mismatch, not proof and not a credible low-mileage-elite conclusion.

The existing row stays in the exploratory comparison, with a visible review note
on its point/profile. No match is silently corrected or excluded. Before any
editorial interpretation, independently reconcile the provider identity and
source match; if a correction is established, rebuild and version affected data
and comparisons. This review is a data-linkage concern, not a claim about either
person's honesty or training.

## Strong continuity examples and the complete inventory

All nine athletes with Runs in all 52 full weeks are Hillary Kipkoech, Sean
Donoghue, Marco Langon, Laura Nagel, Jude Thomas, Tom Anderson, Noah Schutte,
Parker Stokes, and Rachel Smith. Hillary has 51 distance-measured weeks; the other
eight have 52. This is continuity of observed records, not proven completeness.

Sean Donoghue (701 annual Run records; median 119.75 km/week; one warning week)
and Marco Langon (618; 120.77 km/week; two warnings) are useful starting points
for dense, broadly recorded examples. Rachel Smith (220; 44.96 km/week; one
warning) offers a contrasting lower-density year-long record. Laura Nagel is
valuable for the collection-reliability question, not a clean-source reference.

The database contains **460 athletes with any stored activity, 455 with Runs**.
The new `/recordings` page lists all 460, not just seven-event contributors.
It is not all public Strava athletes, current account visibility, or every public
structured workout. No validated workout/session label currently establishes
that last category.

Reproduce the complete local report with:

```sh
.venv/bin/python scripts/audit_recorded_athletes_2024.py
```

Output: `data/derived/2024/p1-exploration/recorded-athlete-audit.md` (Git-ignored).
It contains the shortlist, all nine 52-week entries, and every one of the 460
recorded athletes, with profile links and week/count/warning values. The script
checks that the database SHA-256 is unchanged. Only code and this concise review
are versioned; the full generated inventory stays local.
