# P1 analysis decisions

Status: awaiting discussion. This document records proposals separately from approved analytical choices.

## Scope already agreed

Build a descriptive and comparative study of observed 2024 elite-runner training. Prediction and 2025 matching remain P2. Daniel participates in analytical decisions before they are implemented.

## Evidence gathered

The [reproducible cohort audit](p1-cohort-audit-2024.md) uses the packaged P0 database read-only. It reports aggregate counts, not athlete-level exports. It verifies unique athlete IDs and at most one primary discipline per athlete.

Of 3,609 registry athletes, 585 meet the existing P0 coverage flag; 584 also have a primary event. The 1,100-point source cutoff predates P1 and limits the available population. Its original motivation has not been established by this audit.

The P0 primary event is selected by highest 2024 results score, then performance count, then discipline name. This is reproducible but may differ from an athlete's self-described specialization. P0 defines high coverage as score >=90 and moderate as >=75; these scores summarize collection evidence, not statistical confidence in complete training.

## First decision: event comparisons

Proposal for discussion: start with event-specific views for 800m, 1500m, 5000m, 10000m, steeplechase, half marathon, and marathon. Preserve small road events as descriptive profiles until a defensible comparison is selected. Their sample sizes do not support treating them as equally informative cohorts.

Alternative: use broad groups for the initial study (middle distance, track distance, road distance, marathon separately). This improves sample sizes but combines distinct event demands. Existing P0 source metadata groups marathon under road distance; a separate marathon group would be an explicit P1 analysis choice.

No proposal is approved yet. Ask Daniel which comparison interests him most and use that to choose the first study view.

## Later checkpoints

- Decide whether comparisons are primarily separated by recorded sex; show denominators for both either way. Half marathon has only six eligible female athletes.
- Approve performance tier definitions. The audit's 50-point bands are sample-size diagnostics only; choose fixed bands, quantiles, or continuous scores before interpreting training differences.
- Agree how to handle small cohorts, and distinguish descriptive summaries from inferential comparisons. Do not invent a universal sample-size threshold.
- Retain P0 coverage inclusion as the initial reference and agree on any sensitivity analysis before changing it.
- Agree hypotheses and the first metrics before calculating final comparisons.
- Select editorial athletes after establishing those rules, documenting the selection rather than presenting convenient examples as representative.

## Implementation boundary

Current work is a branch-local audit and planning checkpoint. No analytical groupings, performance tiers, production data, or deployed UI have been changed for P1.
