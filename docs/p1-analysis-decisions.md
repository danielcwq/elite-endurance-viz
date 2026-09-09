# P1 analysis decisions

Status: first event comparison approved; annual-week cutoffs withdrawn. Audit collection evidence and posting patterns before proposing an inclusion protocol.

## Scope already agreed

Build a descriptive and comparative study of observed 2024 elite-runner training. Prediction and 2025 matching remain P2. Daniel participates in analytical decisions before they are implemented.

## Evidence gathered

The [reproducible cohort audit](p1-cohort-audit-2024.md) uses the packaged P0 database read-only. It reports aggregate counts, not athlete-level exports. It verifies unique athlete IDs and at most one primary discipline per athlete.

Of 3,609 registry athletes, 585 meet the existing P0 coverage flag; 584 also have a primary event. The 1,100-point source cutoff predates P1 and limits the available population. Its original motivation has not been established by this audit.

The P0 primary event is selected by highest 2024 results score, then performance count, then discipline name. This is reproducible but may differ from an athlete's self-described specialization. P0 defines high coverage as score >=90 and moderate as >=75; these scores summarize collection evidence, not statistical confidence in complete training.

## First decision: event comparisons

Proposal for discussion: start with event-specific views for 800m, 1500m, 5000m, 10000m, steeplechase, half marathon, and marathon. Preserve small road events as descriptive profiles until a defensible comparison is selected. Their sample sizes do not support treating them as equally informative cohorts.

Alternative: use broad groups for the initial study (middle distance, track distance, road distance, marathon separately). This improves sample sizes but combines distinct event demands. Existing P0 source metadata groups marathon under road distance; a separate marathon group would be an explicit P1 analysis choice.

Approved on 2026-09-09: Daniel accepted 800m versus 1500m as the first comparison. The initial question is how their publicly observed 2024 training patterns differ. Approval selects the two events; it does not approve new coverage thresholds, sex pooling, performance tiers, or statistical hypotheses.

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

The readiness audit also finds 24 runs with missing distance within usable weeks. Existing P0 weekly aggregation fills missing run distance with zero. A P1 volume measure should mark affected athlete-weeks as incomplete for distance, or explicitly label their totals as lower bounds; do not treat these as complete distance measurements. Counts above precede metric-specific exclusions. Activity counts may still be usable when distance is unavailable.

Earlier protocol suggestions, also not approved and pending the evidence audit:

- Show women and men in separate comparison panels (32/76 for 800m and 44/109 for 1500m before the additional floor), using the recorded source classification.
- Start with each athlete's median weekly run distance and run-session count over usable full weeks. Give every athlete equal weight in cohort distributions rather than pooling all athlete-weeks.
- Keep warning/missing weeks visibly distinct on timelines, exclude them from the primary comparison, and preserve observed zeros where measurement completeness permits.
- Use the existing primary-event assignment once per athlete. The audit identifies 56 eligible athletes with performances in both events; do not count them in both cohorts.
- Defer performance tiers until these choices are approved. Specify hypotheses before inspecting comparative training outcomes.

## Later checkpoints

- Decide whether comparisons are primarily separated by recorded sex; show denominators for both either way. Half marathon has only six eligible female athletes.
- Approve performance tier definitions. The audit's 50-point bands are sample-size diagnostics only; choose fixed bands, quantiles, or continuous scores before interpreting training differences.
- Agree how to handle small cohorts, and distinguish descriptive summaries from inferential comparisons. Do not invent a universal sample-size threshold.
- Choose inclusion appropriate to the agreed question after the collection/posting audit; P0 eligibility is a historical diagnostic only.
- Agree hypotheses and the first metrics before calculating final comparisons.
- Select editorial athletes after establishing those rules, documenting the selection rather than presenting convenient examples as representative.

## Implementation boundary

The read-only P1 evidence layer distinguishes ambiguous empty records, activity-backed records, source warnings, activities without weekly records, and no evidence. It retains actual activities irrespective of source-warning status. It does not infer true training completeness, posting intentions, or cohort eligibility. A no-run count is a count of stored records, not zero training.

The first event pair is approved; production data, deployed UI, performance tiers, and analytical inclusion policies have not been changed for P1. The P0 pipeline still needs a separately validated ambiguous-zero correction before its weekly summaries can be reused as P1 analytical truth.
