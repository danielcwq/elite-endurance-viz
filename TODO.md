# EnduranceViz 2024 Snapshot Roadmap

Last repository audit: 2026-09-02

## Scope

This roadmap deliberately treats EnduranceViz as a frozen, reproducible study of publicly observable 2024 elite-runner training.

- **P0:** make the 2024 dataset trustworthy, reproducible, and fast enough to serve.
- **P1:** turn the 2024 snapshot into a hiring-quality descriptive and comparative analytics project.
- **P2:** match 2024 athletes to 2025 outcomes, benchmark prediction models, resume ongoing collection, and add multi-season analysis.

Matching 2024 and 2025 performances is explicitly **not** a P0 requirement and is deferred from the required P1 scope.

## Status legend

- `[x]` — implemented and usable in the current repository.
- `[ ] [PARTIAL]` — meaningful work or an artifact exists, but it does not yet satisfy the roadmap requirement.
- `[ ] [VERIFY]` — requires a product or methodology decision before implementation.
- `[ ] [BLOCKED]` — implementation is ready, but an unavailable external source prevents completion.
- `[ ]` — not implemented or no evidence was found in the repository.

“Partial” is intentionally conservative. Notebook experiments and one-off scripts count as useful prior work, but not as complete when they are not reproducible, tested, or connected to the serving application.

## What already exists and should be reused

- [x] World Athletics result collection and a 1,100-point eligibility threshold exist in [`Get_Data/iaaf.py`](Get_Data/iaaf.py) and the [`OLY24 Pred`](OLY24%20Pred) notebooks.
- [x] The current performance CSV is already at one-row-per-performance grain and its natural key of competitor, discipline, mark, and date is unique.
- [x] Strava weekly collection, raw JSON capture, and activity extraction exist in [`Get_Data/strava_scrape_new.py`](Get_Data/strava_scrape_new.py) and [`Get_Data/test_scraping.py`](Get_Data/test_scraping.py).
- [x] Legacy min/mile-to-min/km conversion and time parsing exist in [`Get_Data/Post Data Collection Processing.ipynb`](Get_Data/Post%20Data%20Collection%20Processing.ipynb).
- [x] Basic activity aggregates—distance, hours, weekly average, and pace—exist in [`Get_Data/data_processing.py`](Get_Data/data_processing.py).
- [x] A weekly consistency prototype exists and produced [`data/metadata/athlete_statistics.csv`](data/metadata/athlete_statistics.csv), including consistency percentage, standard deviation, median-to-mean ratio, and max-to-median ratio.
- [x] Name normalization and athlete-ID mapping were attempted in [`adhoc-processing.py`](adhoc-processing.py).
- [x] MongoDB serving collections, update logs, and a working production deployment exist.
- [x] The current FastHTML app already provides athlete search, a country map, athlete summaries, season-best marks, and raw activity tables in [`main.py`](main.py).
- [x] Earlier Olympic notebooks already explored exponential-decay rankings, comparison with World Athletics rankings, and RMSE against Olympic placement. These are useful research precedents, but are not part of the current training-data pipeline.
- [x] Raw batch, temporary, processed, and timestamped backup artifacts preserve substantial provenance, although they are not organized as an immutable dataset release.

## Important audited gaps

These findings explain why existing prototypes are marked partial rather than complete:

- The repository contains 291 tracked CSVs with 27 distinct schemas.
- The local activity file contains 147,070 rows but only 137,478 unique activity IDs.
- The live Mongo collection contains 150,620 rows but only 139,949 unique activity IDs.
- Production contains 3,550 activity rows that are absent from the checked-in canonical CSV.
- The live headline activity count is inflated by 10,671 duplicate rows.
- The metadata has 460 rows but only 458 unique, valid, nonzero athlete IDs.
- Four activity-side athlete IDs are absent from athlete metadata.
- Six World Athletics competitors map to more than one Strava ID.
- The homepage reports 52 countries because a missing nationality is counted; there are 51 valid country codes.
- The existing weekly merged file has 60,600 rows and 30,075 duplicate athlete-week rows.
- Existing metric code divides by a week number such as 45 or 52, rather than a documented observed-week denominator.
- Incremental deduplication checks only against the prior activity file and does not deduplicate within a newly concatenated batch.
- Mongo has no application indexes or schema validators; athlete activity lookup scans all 150,620 documents and sorts in memory.
- The “test” scripts are manual execution/debug scripts rather than assertion-based automated tests; at least one calls an outdated function signature.
- There is no CI configuration, migration system, data dictionary, or one-command snapshot build.

---

# P0 — Trusted 2024 analytical foundation

## P0.1 Snapshot contract

- [x] Define the canonical half-open window as `2024-01-01T00:00:00Z` through, but excluding, `2025-01-01T00:00:00Z` in [`config/snapshot_2024.yaml`](config/snapshot_2024.yaml). The audited legacy table contains 12 activities from 2023 and 25 from 2025.
- [x] Assign an activity crossing midnight or a boundary to the year/week of its UTC start timestamp.
- [x] Define canonical internal units: meters, seconds, UTC timestamps, and seconds per kilometer.
- [x] Define normalized `Run`, `Ride`, `Swim`, and `Other` categories while preserving the raw Strava type.
- [x] Define “observed week,” “active week,” “missing week,” and “complete-enough week” without treating missing observations as zero training.
- [x] State that the snapshot represents publicly observed Strava activity rather than complete training history.
- [x] Publish these decisions in the versioned [`2024 dataset specification`](docs/data-specification-2024-v1.md).

## P0.2 Preserve and reconcile existing data

- [ ] [BLOCKED] Export the four live Mongo collections into a dated, immutable snapshot. The read-only exporter exists, but Atlas had no usable primary/network connection on 2026-09-02; see [`source reconciliation status`](data/manifests/source_reconciliation_status_2026-09-02.md).
- [x] Preserve the 291 current repository CSVs as immutable Git inputs with file-level SHA-256 checksums in [`repository_csv_snapshot_2026-09-02.csv`](data/manifests/repository_csv_snapshot_2026-09-02.csv).
- [ ] [BLOCKED] Reconcile the previously observed 3,550 production-only activity rows with the repository snapshot. The tested reconciliation command is ready and awaits the Mongo export.
- [ ] [PARTIAL] Generate manifests containing source file, checksum, row count, schema signature, extraction time, and source system. The repository manifest is complete; the Mongo manifest awaits a successful export.
- [x] Ensure raw snapshot exports are never modified by cleaning code: the exporter is write-once and makes completed payloads read-only.
- [x] Separate raw, staging, curated, derived, manifest, and quarantine outputs and document their write policies in [`data/README.md`](data/README.md).
- [x] Keep generated raw JSON out of Git; commit checksums/manifests and retain payload bundles in versioned object storage or redundant offline storage.

Suggested structure:

```text
data/
├── raw/2024/
├── staging/
├── curated/2024/
├── derived/2024/
├── manifests/
└── quarantine/
```

## P0.3 Canonical data model

- [x] Define an `athletes` table that separates canonical identity from source attributes and derived metrics.
- [x] Define provider-aware `athlete_external_accounts` with uniqueness enforcement on `(provider, external_account_id)`.
- [x] Define strongly typed `performances_2024` with a 2024 date constraint and natural-field uniqueness.
- [x] Define strongly typed, one-row-per-Strava-ID `activities_2024` with canonical units and identity foreign keys.
- [x] Define one-row-per-athlete-week `weekly_training_2024` with sport-specific counts, distances, and durations.
- [x] Define `data_coverage_2024` with explicit observed, missing, and unknown states.
- [x] Define `import_manifest` linked to a canonical build.
- [x] Define `quarantined_records` with reason codes, original JSON, and source references.
- [x] Choose Parquet plus DuckDB for the canonical analytical engine; PostgreSQL remains a possible downstream serving layer.
- [x] Document and test primary keys, foreign keys, nullability, units, and uniqueness rules in [`schema/2024.sql`](schema/2024.sql) and the [`2024 data dictionary`](docs/data-dictionary-2024.md).

## P0.4 Canonical athlete identity

- [ ] Generate an immutable internal athlete ID that is independent of Strava and display names.
- [ ] [PARTIAL] Preserve official World Athletics names separately from Strava display names. Both fields exist today, but they are joined and queried by name.
- [ ] [PARTIAL] Create a provider-aware mapping from internal athlete ID to Strava athlete ID. Existing mapping logic in [`adhoc-processing.py`](adhoc-processing.py) uses normalized names and fills unmatched IDs with zero.
- [ ] Resolve Jack Rayner and Mario García Romo, whose metadata currently uses athlete ID `0`.
- [ ] Resolve activity-side IDs for Jack Rayner, Mario García Romo, Kevin Robertson, and Wes Ferguson that are absent from current athlete metadata.
- [ ] Resolve the six competitors mapped to multiple Strava IDs: Jason Pointeau, Jessica McClain, Margaux Sieracki, Maureen Koster, Zhixuan Li, and Águeda Marqués.
- [ ] Normalize whitespace, capitalization, accents, and known aliases for matching only—not as permanent identifiers.
- [ ] Quarantine ambiguous mappings rather than selecting one by filename or row order.
- [ ] Add uniqueness tests for `(provider, external_account_id)`.
- [ ] Add referential-integrity tests from activities and performances to athletes.

## P0.5 Activity cleaning and deduplication

- [ ] Filter the curated activity table to the canonical 2024 window.
- [ ] [PARTIAL] Deduplicate on Strava `Activity ID`. Existing code rejects IDs already present in the prior CSV, but duplicates within a new concatenated batch survive.
- [ ] Separate exact duplicate groups from conflicting duplicate groups.
- [ ] Resolve conflicting duplicates using documented completeness and provenance rules.
- [ ] Quarantine conflicts involving athlete, timestamp, distance, duration, or activity type.
- [ ] Remove `Serial` from the canonical model; it is unique but contains gaps and has no source meaning.
- [ ] [PARTIAL] Parse `Start Date` to a typed UTC timestamp. The notebook parses timestamps, but the final CSV and Mongo documents store strings.
- [ ] [PARTIAL] Normalize distance units. Min/mile and min/km conversion logic exists, but it is notebook-driven and retains redundant legacy columns.
- [ ] Separate elapsed time, moving time, and display time where source data permits.
- [ ] Preserve raw source values beside curated values or through source-record references.
- [ ] Stop coercing unknown values to `0`; use null plus a quality reason.
- [ ] [PARTIAL] Normalize activity types using one shared taxonomy across batch processing, weekly aggregation, and serving.
- [ ] Include `Run`, `TrailRun`, and `VirtualRun` according to the documented run-category rule.
- [ ] Include `Ride`, `VirtualRide`, mountain bike, e-bike, and related activities according to the documented ride-category rule.
- [ ] Flag, rather than silently delete, suspicious distance, time, and pace values.
- [ ] Add `quality_status`, `quality_flags`, and `exclusion_reason` fields.
- [ ] Recalculate duplicate inflation by athlete and store it in the audit report.

## P0.6 Normalize 2024 performances

- [x] Preserve one row per World Athletics performance in [`data/metadata/master_iaaf_database_with_strava.csv`](data/metadata/master_iaaf_database_with_strava.csv).
- [x] Preserve raw mark, discipline, date, location, nationality, gender, and World Athletics result score.
- [x] Generate a natural performance identifier in [`Get_Data/iaaf.py`](Get_Data/iaaf.py), based on competitor, discipline, mark, and date.
- [ ] Remove the legacy `Unnamed: 0` pseudo-identifier from the canonical model.
- [ ] Parse performance dates into a typed date.
- [ ] Preserve the raw mark string and parse comparable marks into seconds where the discipline permits.
- [ ] Link every curated performance to the canonical internal athlete ID or quarantine it.
- [ ] Define a deterministic primary-2024-discipline rule, preferably using the highest 2024 World Athletics score with documented tie-breaking.
- [ ] Define the 2024 season-best performance per athlete and discipline.
- [ ] Enforce natural uniqueness on athlete, discipline, mark, date, and venue/source.
- [ ] Replace pipe-delimited `Mark` and `Discipline` fields in athlete metadata with queries against performance rows.
- [ ] Correct the three known cases where pipe-delimited mark counts and discipline counts do not align.

## P0.7 Coverage model

- [ ] [PARTIAL] Calculate first and last observed activity dates. The data supports this, but it is not materialized or displayed.
- [ ] [PARTIAL] Calculate observed weeks. `2024 Weeks Scraped` exists, but values represent manual processing state rather than verified calendar coverage.
- [ ] Calculate active weeks separately from observed weeks.
- [ ] Distinguish an observed zero-activity week from an unobserved/missing week.
- [ ] Detect likely collection gaps and partially processed athlete ranges.
- [ ] Calculate unique activities per athlete-week after deduplication.
- [ ] [PARTIAL] Reuse the existing consistency prototype as a reference, not as the final coverage definition.
- [ ] Create a coverage score with documented components.
- [ ] Create human-readable coverage statuses such as `high`, `moderate`, `low`, and `insufficient`.
- [ ] [VERIFY] Select cohort inclusion thresholds only after auditing the coverage distribution.
- [ ] Keep low-coverage athletes discoverable but exclude them from default cohort claims.

## P0.8 Weekly analytical table

Grain: one canonical athlete by one 2024 calendar week.

- [ ] [PARTIAL] Materialize unique `(athlete_id, week_start)` rows. Weekly metadata exists, but 30,075 current rows are duplicate athlete-week extras.
- [ ] Calculate run distance and duration.
- [ ] Calculate unique run count.
- [ ] Calculate active days.
- [ ] Calculate double-session days.
- [ ] Calculate longest run.
- [ ] Calculate long-run share.
- [ ] Calculate ride duration.
- [ ] Calculate swim duration.
- [ ] Calculate strength count and duration where available.
- [ ] Calculate other cross-training duration.
- [ ] Calculate total observed training duration.
- [ ] Calculate cross-training share.
- [ ] Attach week-level coverage status.
- [ ] Calculate four-week rolling metrics with explicit handling of missing weeks.
- [ ] Calculate week-over-week change.
- [ ] Reconcile every weekly total to the curated activity table.

## P0.9 Athlete summary metrics

- [ ] [PARTIAL] Recalculate total 2024 run distance and hours from deduplicated activities. Existing totals are available but include duplicate inflation.
- [ ] Replace ambiguous “average weekly mileage” with explicitly named observed-week and/or calendar-week measures.
- [ ] Calculate median weekly distance.
- [ ] Calculate peak week.
- [ ] Calculate peak four-week average.
- [ ] Calculate weekly variation and consistency.
- [ ] Calculate active-day frequency.
- [ ] Calculate long-run profile.
- [ ] Calculate cross-training composition.
- [ ] [PARTIAL] Review average pace. The current weighted calculation exists, but the source time semantics and anomalous values are not sufficiently controlled.
- [ ] Attach window, denominator, coverage, and dataset version to every public metric.
- [ ] Reconcile aggregate values against both weekly and activity-grain tables.

## P0.10 Automated data-quality tests

- [ ] [PARTIAL] Convert the existing manual scripts under `Get_Data/test_*.py` into assertion-based tests. They currently print/debug, write files, and include outdated calls.
- [ ] Fail on duplicate curated activity IDs.
- [ ] Fail on duplicate provider/external-athlete IDs.
- [ ] Fail on broken athlete foreign keys.
- [ ] Fail on unparseable required timestamps.
- [ ] Fail on curated activities outside the snapshot window.
- [ ] Fail on negative distance or duration.
- [ ] Fail on inconsistent canonical units.
- [ ] Fail on duplicate athlete-week keys.
- [ ] Fail when weekly totals do not reconcile with activities.
- [ ] Fail when athlete summaries do not reconcile with weekly totals.
- [ ] Fail on mismatched performance mark/discipline representations.
- [ ] Detect unexpected row-count or coverage regressions.
- [ ] Produce a human-readable data-quality report for each dataset build.
- [ ] Add a small fixture dataset containing known duplicates, identity conflicts, missing weeks, and invalid measurements.

## P0.11 Reproducible pipeline

- [ ] [PARTIAL] Extract production transformations from notebooks into tested Python modules or SQL. The logic exists, but execution order and working directories are manual.
- [ ] Create one command to build curated data from the immutable raw snapshot.
- [ ] Create one command to validate the curated snapshot.
- [ ] Create one command to populate a local serving database.
- [ ] Pin all required dependencies, including dependencies currently imported but absent from `requirements.txt`.
- [ ] Make the build idempotent.
- [ ] Remove dependence on current working directory for file resolution.
- [ ] Record dataset version, code version, source manifest, and build time.
- [ ] Ensure a new developer can rebuild the snapshot without production or Strava credentials.
- [ ] Keep historical notebooks as research/provenance artifacts while removing them from the required production path.

## P0.12 Serving and backend correctness

- [x] Serve the application through FastHTML and Vercel.
- [x] Connect to the `elite_endurance` Mongo database.
- [x] Display athlete metadata and activities.
- [x] Record and display Mongo update-log counts and timestamps.
- [ ] Route and query athletes by internal athlete ID or a stable slug.
- [ ] Replace case-insensitive name-regex joins with ID joins.
- [ ] Add a unique index on the source activity ID.
- [ ] Add an index on athlete ID and start timestamp descending.
- [ ] Add an index or canonical lookup for athlete slug/ID.
- [ ] Add activity pagination or incremental loading, initially 30–50 rows.
- [ ] Add field projections so detail pages retrieve only displayed data.
- [ ] Cache public homepage and summary responses.
- [ ] Stop embedding all 460 athlete documents in homepage HTML.
- [ ] Replace drop-and-rebuild Mongo refreshes with an atomic/versioned swap if Mongo remains the serving database.
- [ ] Prevent [`mongodb_init/db_upload.py`](mongodb_init/db_upload.py) from blindly appending duplicate copies.
- [ ] Correct displayed athlete, country, and activity counts.
- [ ] Display `2024 Snapshot`, dataset version, build date, and coverage prominently.
- [ ] [VERIFY] Decide whether Mongo remains the serving database or is replaced by PostgreSQL after the curated model is stable.

## P0.13 Documentation

- [ ] [PARTIAL] Rewrite the root [`README.md`](README.md) around the 2024 snapshot scope. The existing README explains the original motivation and collection idea, but not the current architecture or dataset limitations.
- [ ] Publish a data dictionary.
- [ ] Publish a source-to-curated lineage diagram.
- [ ] Publish metric definitions.
- [ ] Publish identity-resolution rules.
- [ ] Publish deduplication and conflict-resolution rules.
- [ ] Publish known limitations and public-posting bias.
- [ ] Publish snapshot build instructions.
- [ ] Include an example quality report.
- [ ] Document which legacy artifacts are provenance-only and which files remain authoritative.

## P0 exit criteria

- [ ] Every curated activity has one unique source activity ID.
- [ ] Every curated activity and performance references one canonical athlete or is quarantined.
- [ ] Weekly totals reconcile with curated activities.
- [ ] Athlete summaries reconcile with weekly totals.
- [ ] The 2024 snapshot rebuilds from immutable inputs with one documented command.
- [ ] Automated tests and the quality report pass.
- [ ] The site displays accurate deduplicated counts and honest coverage.
- [ ] Athlete activity queries are indexed and paginated.
- [ ] Nothing on the site implies that the snapshot is current live training data.

---

# P1 — Hiring-quality 2024 analytics project

Required P1 scope is descriptive and comparative. Prediction models and 2025 outcome matching remain P2.

## P1.1 Analytical question and cohorts

- [ ] Frame the primary question: “How did publicly observable 2024 training patterns differ across elite running disciplines and performance levels?”
- [ ] [PARTIAL] Reuse the existing 1,100 World Athletics point threshold, but document why it was chosen and how it affects selection.
- [ ] Define event groups such as middle distance, track distance, road distance, and marathon.
- [ ] Use the P0 primary-discipline rule to assign each athlete to a default cohort.
- [ ] Define World Athletics score bands.
- [ ] Use P0 coverage thresholds for default inclusion.
- [ ] Audit cohort sizes before selecting comparisons.
- [ ] Ensure one athlete appears only once in a single cohort analysis.
- [ ] Publish inclusion, exclusion, and low-coverage counts.
- [ ] Predefine analysis questions and metrics before interpreting results.

## P1.2 Athlete training fingerprint

- [ ] [PARTIAL] Reuse the existing athlete profile route, search, season-best display, summary cards, and activity table as the shell.
- [ ] Add a 2024 weekly volume timeline.
- [ ] Add four-week rolling volume.
- [ ] Add weekly run-frequency distribution.
- [ ] Add longest-run timeline and long-run share.
- [ ] Add cross-training composition.
- [ ] Add active days and double-session frequency.
- [ ] Add consistency and variability measures.
- [ ] Identify peak observed training periods.
- [ ] Display structured 2024 season-best performances.
- [ ] Display coverage and confidence beside every profile.
- [ ] Add date and activity-category filters.
- [ ] Add shareable chart state or stable filtered URLs.
- [ ] Ensure missing data renders as missing, not zero.

## P1.3 Cohort comparison

- [ ] Compare an athlete with their default event cohort.
- [ ] Show cohort median and interquartile range rather than only ranks.
- [ ] Show athlete percentiles for supported metrics.
- [ ] Compare weekly volume.
- [ ] Compare activity frequency.
- [ ] Compare long-run share.
- [ ] Compare cross-training composition.
- [ ] Compare consistency.
- [ ] Allow performance-tier filtering.
- [ ] Warn or suppress inference when cohort size is too small.
- [ ] Exclude insufficient-coverage athletes by default while making the exclusion visible.
- [ ] Make cohort definitions and denominators accessible from the comparison view.

## P1.4 Coherent 2024 study

- [ ] [PARTIAL] Reuse the research style of [`OLY24 Pred`](OLY24%20Pred), which already compares rankings and reports RMSE, but rebuild the study on the curated training dataset.
- [ ] Define three initial questions:
  - How does observed training volume differ by primary event?
  - How does observed training composition differ by event?
  - Are 2024 performance tiers associated with different observed training patterns?
- [ ] Write hypotheses before calculating final comparisons.
- [ ] Produce cohort descriptive statistics.
- [ ] Visualize distributions, not just means.
- [ ] Report uncertainty intervals and effect sizes.
- [ ] Stratify or control for data coverage.
- [ ] Investigate influential outliers and duplicate-related historical distortions.
- [ ] Include counterexamples and unsupported hypotheses.
- [ ] Avoid causal or coaching-prescription claims.
- [ ] [VERIFY] Decide whether a small explanatory regression adds value; it is optional and must not turn P1 into a prediction-model project.
- [ ] Publish the study as a navigable narrative, not only a notebook.

## P1.5 Editorial case studies

- [ ] Select three to five high-coverage athletes across different event groups.
- [ ] Explain each athlete’s 2024 training fingerprint.
- [ ] Compare each athlete with an appropriate cohort.
- [ ] Identify distinctive patterns without implying causation or prescribing training.
- [ ] Include at least one surprising or contradictory case.
- [ ] Make each case study shareable with a stable URL and summary graphic.

## P1.6 Homepage and product narrative

- [ ] [PARTIAL] Reuse the current map, search, stats/about toggle, and production deployment where useful.
- [ ] Lead with the 2024 study question rather than database counts.
- [ ] Feature two or three defensible findings.
- [ ] Provide direct entry into athlete fingerprints and cohort comparison.
- [ ] Explain what the snapshot contains and does not contain.
- [ ] Feature one editorial case study.
- [ ] Retain dataset counts only as supporting, deduplicated context.
- [ ] Add a meaningful homepage title, description, and share metadata.

## P1.7 Methodology and transparency

- [ ] Publish a methodology page.
- [ ] Publish cohort construction and flow.
- [ ] Visualize the coverage distribution.
- [ ] Publish the P0 quality summary.
- [ ] Publish a metric glossary.
- [ ] Explain selection bias and missing-not-at-random public Strava posting.
- [ ] Distinguish observable public activity from complete athlete training.
- [ ] Show dataset version and build date on every analytical page.
- [ ] Link every chart to its metric definition and inclusion rules.

## P1.8 Engineering portfolio quality

- [ ] Add automated CI for unit tests, data fixtures, and schema/quality checks.
- [ ] Provide a small non-sensitive seed dataset.
- [ ] Provide one-command local setup and one-command test execution.
- [ ] Publish an architecture diagram.
- [ ] Publish a transformation/data-lineage diagram.
- [ ] Publish a database or curated-schema diagram.
- [ ] Publish before-and-after database-query measurements.
- [ ] Publish before-and-after payload-size and page-latency measurements.
- [ ] Run a permitted staging load test and publish the methodology and results.
- [ ] Add structured logging and an application health endpoint.
- [ ] Handle missing, excluded, low-coverage, and empty-cohort states intentionally.
- [ ] Rewrite the README as a technical and analytical case study.
- [ ] Add screenshots or a short demo walkthrough.
- [ ] Explain major engineering tradeoffs and rejected alternatives.
- [ ] Keep FastHTML unless a concrete analytical or maintainability requirement justifies a rewrite.

## P1.9 Product QA

- [ ] Test high-, moderate-, low-, and insufficient-coverage athletes.
- [ ] Test athletes with missing performance metadata and unusual activity types.
- [ ] Test empty and undersized cohorts.
- [ ] Verify every chart against curated-table queries.
- [ ] Test mobile and narrow-table behavior.
- [ ] Test keyboard navigation and accessible chart labels.
- [ ] Verify page titles and share metadata.
- [ ] Verify stable filtered/shareable URLs.
- [ ] Validate that missing data never becomes a misleading zero.
- [ ] Validate that low-coverage warnings remain visible in screenshots and shared views.

## P1 exit criteria

- [ ] The homepage communicates the study question and product value in under one minute.
- [ ] Every displayed metric has a definition, window, denominator, and coverage status.
- [ ] Athlete and cohort charts reconcile with curated P0 tables.
- [ ] At least one coherent 2024 analytical study is published.
- [ ] At least three athlete case studies are shareable.
- [ ] The repository demonstrates reproducible data engineering, testing, performance work, and analytical judgment.
- [ ] A sports-analytics or data-engineering hiring manager can explore the project without a guided explanation.

---

# P2 — Explicitly deferred

- [ ] Normalize and ingest 2025 performance results.
- [ ] Match 2024 athlete identities to 2025 results.
- [ ] Define 2025 prediction targets and missing-outcome semantics.
- [ ] Build persistence and discipline baselines.
- [ ] Implement grouped/nested cross-validation and locked evaluation data.
- [ ] Benchmark regularized linear, additive, and tree-based models.
- [ ] Publish calibration, subgroup performance, uncertainty, and error analysis.
- [ ] Generalize race-build-up and taper analysis.
- [ ] Resume ongoing/current data collection.
- [ ] Add multi-season storage and versioning.
- [ ] Add heart rate, elevation, power, lap, or workout-classification data where legitimately and consistently available.

---

# Recommended execution order

## Phase 1: freeze and reconcile

1. P0.1 snapshot contract.
2. P0.2 raw export, manifest, and production reconciliation.
3. P0.3 canonical schema.

## Phase 2: make records trustworthy

1. P0.4 athlete identity.
2. P0.5 activity cleaning and deduplication.
3. P0.6 2024 performance normalization.
4. P0.7 coverage model.

## Phase 3: make analysis reproducible

1. P0.8 weekly table.
2. P0.9 athlete metrics.
3. P0.10 automated quality tests.
4. P0.11 reproducible pipeline.

## Phase 4: make the existing app honest and fast

1. P0.12 ID-based serving, indexes, pagination, caching, and accurate labels.
2. P0.13 documentation.
3. Verify all P0 exit criteria.

## Phase 5: build the hiring project

1. P1.1 questions and cohorts.
2. P1.2 athlete training fingerprint.
3. P1.3 cohort comparison.
4. P1.4 study and P1.5 case studies.
5. P1.6–P1.9 narrative, transparency, engineering polish, and QA.

# Estimated lift

- **P0:** approximately 80–130 focused engineering hours.
- **P1:** approximately 90–150 focused engineering/analytics hours.
- **Combined:** approximately 170–280 hours, or roughly 4.5–7 full-time weeks.

The largest uncertainties are identity conflict resolution, production/repository reconciliation, and interpreting time/pace fields. The main opportunities for reuse are the existing World Athletics collection code, Strava activity parser, conversion helpers, weekly consistency prototype, FastHTML routes, and the earlier Olympic analytical notebooks.
