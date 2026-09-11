# P1 analysis decisions

Status: broader event-comparison plan and continuous-point performance representation approved; exploratory analysis resumed. Annual-week cutoffs remain withdrawn. Final inclusion and inference policies are not approved.

## Scope already agreed

Build a descriptive and comparative study of observed 2024 elite-runner training. Prediction and 2025 matching remain P2. Daniel participates in analytical decisions before they are implemented.

## Evidence gathered

### Latest presentation and audit request — 2026-09-10

After reviewing the first preview, Daniel requested overlaid charts with hover
highlighting and women/men selection, a broader UI refresh, an explanation of
event assignment and missing marathon comparisons, and proposed anomaly cases.
This supersedes the earlier stop-development instruction only for this work.

- The preview overlays distinct event/recorded-sex series on a shared axis, not
  pooled observations or statistics. The sex selector restricts displayed series.
  Distribution curves are empirical cumulative distributions of the same
  per-athlete medians, not smoothed density estimates.
- Marathon is added to the preview: 1,115 assigned registry athletes, 89 with
  recorded-running-week metrics (29 women, 60 men). The six-event static analysis
  remains unchanged; its 361 contributors become 450 in the seven-event preview.
- “Assigned event” replaces language implying a verified main specialty. The rule
  remains highest stored event result points, then stored result count, then event
  name alphabetically, with missing points last. Stored alternative events are
  visible. No assignment or packaged database contents are changed.
- `/recordings` inventories every athlete with a stored activity (460, including
  455 with Runs). It is not a complete list of public Strava users or workouts.
- The proposed shortlist is purposive, not a representative sample or new cohort
  policy. The linked Kate Mitchell account has a possible identity mismatch;
  visible review notes retain the existing row without claiming the link is wrong
  or silently excluding it. Identity validation must precede an athlete case study.

See [the review and candidate questions](p1-ui-and-recording-review.md). No new
session definition, training-completeness claim, inference, or deployment follows
from this presentation change.

### Earlier audits

The [reproducible cohort audit](p1-cohort-audit-2024.md) uses the packaged P0 database read-only. It reports aggregate counts, not athlete-level exports. It verifies unique athlete IDs and at most one primary discipline per athlete.

Of 3,609 registry athletes, 585 meet the existing P0 coverage flag; 584 also have a primary event. The 1,100-point source cutoff predates P1 and limits the available population. Its original motivation has not been established by this audit.

The P0 primary event is selected by highest 2024 results score, then performance count, then discipline name. This is reproducible but may differ from an athlete's self-described specialization. P0 defines high coverage as score >=90 and moderate as >=75; these scores summarize collection evidence, not statistical confidence in complete training.

## First decision: event comparisons

Proposal for discussion: start with event-specific views for 800m, 1500m, 5000m, 10000m, steeplechase, half marathon, and marathon. Preserve small road events as descriptive profiles until a defensible comparison is selected. Their sample sizes do not support treating them as equally informative cohorts.

Alternative: use broad groups for the initial study (middle distance, track distance, road distance, marathon separately). This improves sample sizes but combines distinct event demands. Existing P0 source metadata groups marathon under road distance; a separate marathon group would be an explicit P1 analysis choice.

Approved on 2026-09-09: Daniel accepted 800m versus 1500m as the first comparison. The initial question is how their publicly observed 2024 training patterns differ. Approval selects the two events; it does not approve new coverage thresholds, sex pooling, performance tiers, or statistical hypotheses.

Updated on 2026-09-09: Daniel proposed a broader event ladder and then approved resuming analysis. The main comparison is now 800m versus 5000m; 1500m supplies context, 5000m versus 10000m extends the track comparison, steeplechase remains separate, and half marathon is a cautious extension. Do not pool steeplechase with 5000m or track 10000m with road 10 km. The study question is how publicly observed running volume, frequency, and session characteristics vary across these event specializations. This supersedes the earlier 800m/1500m-only starting plan.

### First exploratory calculation, specified before inspecting training differences

This is an exploratory description, not a confirmatory hypothesis test or a finalized study protocol. The event ladder and permission to begin are approved. Daniel also explicitly approved separate women's/men's panels, one median summary per athlete over recorded-running weeks, and visible posting coverage. The calculation definitions below were disclosed before inspecting training differences:

- Retain the existing primary-event assignment, one row per athlete, and show recorded-sex groups separately rather than pooling them.
- Count stored Run records. For weekly distance, sum recorded run distances only when every stored run in that week has a finite, nonnegative distance. Otherwise leave the whole-week distance unavailable and expose the measurement count. A recorded zero distance remains zero; a missing distance does not.
- Summarize full Monday–Sunday UTC weeks with at least one recorded Run. These are **recorded-running-week** summaries, not a typical training week or annual training estimate. December 30–31 remains in annual record counts but not full-week summaries.
- Within each athlete, calculate the median recorded-week run count and median measurement-complete recorded-week distance. Then show the distribution of those athlete summaries, giving each contributing athlete one entry. Frequency and distance may have different denominators; show both.
- Apply no minimum annual run/week count and no P0 coverage eligibility filter. Retain source-warning weeks with real activities, count them separately, and do not interpret the absence of a warning as complete capture.
- Report contributing athletes and weeks alongside the values. No p-values, confidence intervals, rankings, performance tiers, causal claims, or headline findings are approved by this exploratory pass.

The main limitation is explicit: conditioning on recorded-running weeks, and on available distance measurements, can select different portions of different athletes' training. These summaries are for inspecting the data and refining the study, not asserting population differences in complete training.

### Recording granularity follow-up

The [run-record audit](p1-run-record-structure-2024.md) inspected the three highest athlete median record counts. Ruken Tek's 39-record weekly median corresponds to six recorded running days; January 5 contains 29 distinct records, many approximately 32 metres long. This is consistent with separately recorded repetitions, not proof of 29 independent sessions. Exact repeated fingerprints were not found, but that does not rule out near-duplicates or overlaps.

Policy v1.1 adds recorded running days (distinct UTC dates with at least one Run) as a complementary diagnostic. It does not change the approved record-count or distance summaries. No short-run exclusion, time-gap clustering, or inferred session count is approved. P0's count-based `double_session_days` must not be presented as validated training-session frequency in P1. A definition of independent sessions is a separate human checkpoint if that measure is pursued.

## Superseded proposal: usable-week inclusion

The selected comparison has 261 athletes under the P0 high/moderate coverage flag. The score combines 70% observed-week breadth with 30% usable/observed-week consistency. An observed week may carry a warning, so a score of 75 is not equivalent to 39 usable weeks. In this comparison the minimum is nine usable weeks, despite a median of 48–49.5 within the event/sex cells.

Historical candidate floors (withdrawn; retained here to explain the earlier discussion):

| Event | Recorded sex | P0 eligible | At least 26 usable weeks | At least 39 usable weeks |
| --- | --- | --- | --- | --- |
| 800m | female | 32 | 30 | 27 |
| 800m | male | 76 | 73 | 69 |
| 1500m | female | 44 | 43 | 39 |
| 1500m | male | 109 | 107 | 103 |
| Total | | 261 | 253 | 238 |

Withdrawn after discussion: 26 and 39 were fractions of a year, not evidence-derived thresholds. We have not selected an annual training question, and P0's week score confuses ambiguous empty collections with successful observations. These floors should not drive P1 inclusion. The [collection/posting audit](p1-observability-audit-2024.md) now considers every athlete assigned to the two events, with no coverage cutoff.

Clarification: there is no requirement to choose any fraction of a year. Decide the study question and time window first, then evaluate the observations needed for that question. The current collection correction needs no analytical cutoff or athlete-exclusion decision.

The readiness audit also finds 24 runs with missing distance within usable weeks. Existing P0 weekly aggregation fills missing run distance with zero. A P1 volume measure should mark affected athlete-weeks as incomplete for distance, or explicitly label their totals as lower bounds; do not treat these as complete distance measurements. Counts above precede metric-specific exclusions. Activity counts may still be usable when distance is unavailable.

Historical protocol suggestions (superseded wherever the approved exploratory definitions above differ):

- Show women and men in separate comparison panels (32/76 for 800m and 44/109 for 1500m before the additional floor), using the recorded source classification.
- Start with each athlete's median weekly run distance and run-session count over usable full weeks. Give every athlete equal weight in cohort distributions rather than pooling all athlete-weeks.
- Keep warning/missing weeks visibly distinct on timelines, exclude them from the primary comparison, and preserve observed zeros where measurement completeness permits.
- Use the existing primary-event assignment once per athlete. The audit identifies 56 eligible athletes with performances in both events; do not count them in both cohorts.
- Defer performance tiers until these choices are approved. Specify hypotheses before inspecting comparative training outcomes.

## Later checkpoints

### Performance representation approved

On 2026-09-09, Daniel explicitly selected **continuous World Athletics points within each event and recorded-sex group**, rather than score bands. This approves the representation for descriptive performance-versus-recorded-training association, not prediction, causation, sex pooling, a new eligibility threshold, or an inferential method. It supersedes the proposed score-band decision and the roadmap's performance-tier controls. Any later performance filter should be an explicit continuous score range, not invented tiers.

The first calculation uses each athlete's highest non-null stored 2024 result score **within their existing primary event**, disclosed before calculating the pairs. Multiple performances and ties never duplicate an athlete. No other-event fallback is used. Athletes missing a score or the relevant training metric stay in the inventory but do not contribute to that metric's paired plot. The existing recorded-week definitions are unchanged.

Implemented on the P1 branch in the [reproducible continuous-points exploration](p1-performance-training-2024.md), with local distance and Run-record scatterplots, one panel per event/recorded-sex group. Colour encodes metric-contributing weeks on a continuous 1–52 scale, not confidence or inclusion. All 2,297 registry athletes in the selected events have a primary-event score; 361 have paired recorded-week values for each metric, though their distance/frequency week denominators can differ. The remaining 1,936 have no full recorded-running-week summary, not missing scores. No numerical association statistic or fitted line is calculated. These are exploratory views, not published study conclusions or new eligibility policy.

### Remaining checkpoints

On 2026-09-10 Daniel narrowed the finish line to the comparison preview and static
plots, with no athlete case studies and no further development beyond that review
package. This resolves the earlier sequencing question. `/compare` retains all
metric-available contributors, shows registry/contributor counts and recording
breadth, and remains exploratory. The historical broader P1 checklist is deferred,
not an automatic continuation queue. No final inclusion policy, percentile
rankings, fitted models, study conclusions, or production deployment is approved.

The [source and calendar audit](p1-source-conflicts-2024.md) traced all 611 conflicting full athlete-week keys in the chosen events. Most pair `No Data` with another summary; some contain the wrong date range, and some differ in displayed totals. No source warnings were silently cleared. Across all event-assigned athletes, 84 in 800m and 119 in 1500m have any stored 2024 Run. This is an observation inventory, not an eligible sample for a study. Monthly counts and gaps are documented without year-fraction filters.

The earlier focus checkpoint is superseded by approval of the broader event ladder and exploratory analysis above. Next, review the provisional recorded-week summaries and settle the final comparison estimand, inclusion policy, and presentation before publishing study conclusions. No annual-week cutoff is presumed.

- Recorded-sex separation is approved for the exploratory view. Do not introduce pooled comparisons without revisiting that choice. Half marathon currently has three female contributors with recorded runs, not the six suggested by the legacy P0 eligibility count.
- Continuous performance points are now approved. The audit's historical 50-point bands remain sample-size diagnostics, not adopted analysis groups.
- Agree how to handle small cohorts, and distinguish descriptive summaries from inferential comparisons. Do not invent a universal sample-size threshold.
- Choose inclusion appropriate to the agreed question after the collection/posting audit; P0 eligibility is a historical diagnostic only.
- Agree hypotheses and the first metrics before calculating final comparisons.
- Editorial athlete case studies were removed from scope on 2026-09-10.

## Implementation boundary

The read-only P1 evidence layer distinguishes ambiguous empty records, activity-backed records, source warnings, activities without weekly records, and no evidence. It retains actual activities irrespective of source-warning status. It does not infer true training completeness, posting intentions, or cohort eligibility. A no-run count is a count of stored records, not zero training.

The event ladder and exploratory view are approved; production data, deployed UI, performance tiers, and final analytical inclusion policies have not been changed for P1. An [opt-in correction is locally verified](p1-evidence-correction-2024.md) with `scripts/build_p1_evidence_2024.py`; it rebuilds only local, Git-ignored weekly evidence and metrics. It does not replace the packaged P0 database or recompute annual summaries/cohort eligibility. The [new exploratory calculation](p1-exploratory-training-2024.md) uses actual activity records, not P0 synthetic zeros or its annual summary formulas, and adds explicit distance-measurement completeness.
