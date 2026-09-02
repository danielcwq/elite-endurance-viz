-- EnduranceViz 2024 canonical analytical schema
-- Specification: docs/data-specification-2024-v1.md

CREATE TABLE dataset_builds (
    build_id UUID PRIMARY KEY,
    dataset_name VARCHAR NOT NULL CHECK (dataset_name = 'enduranceviz-2024'),
    specification_version VARCHAR NOT NULL,
    source_commit VARCHAR NOT NULL,
    started_at_utc TIMESTAMPTZ NOT NULL,
    completed_at_utc TIMESTAMPTZ,
    status VARCHAR NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    output_root VARCHAR NOT NULL
);

CREATE TABLE import_manifest (
    manifest_id VARCHAR PRIMARY KEY,
    build_id UUID NOT NULL REFERENCES dataset_builds(build_id),
    source_file VARCHAR NOT NULL,
    source_system VARCHAR NOT NULL,
    source_role VARCHAR NOT NULL,
    sha256 VARCHAR NOT NULL CHECK (length(sha256) = 64),
    byte_count UBIGINT NOT NULL,
    row_count UBIGINT NOT NULL,
    schema_signature VARCHAR NOT NULL CHECK (length(schema_signature) = 64),
    extracted_at_utc TIMESTAMPTZ NOT NULL,
    UNIQUE (build_id, source_file, sha256)
);

CREATE TABLE athletes (
    athlete_id UUID PRIMARY KEY,
    official_name VARCHAR NOT NULL,
    official_name_normalized VARCHAR NOT NULL,
    display_name VARCHAR,
    nationality_code VARCHAR CHECK (nationality_code IS NULL OR length(nationality_code) = 3),
    gender VARCHAR NOT NULL CHECK (gender IN ('female', 'male', 'other', 'unknown')),
    identity_status VARCHAR NOT NULL CHECK (identity_status IN ('resolved', 'ambiguous', 'unmatched')),
    identity_source VARCHAR NOT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL,
    updated_at_utc TIMESTAMPTZ NOT NULL
);

CREATE TABLE athlete_external_accounts (
    provider VARCHAR NOT NULL,
    external_account_id VARCHAR NOT NULL,
    athlete_id UUID NOT NULL REFERENCES athletes(athlete_id),
    provider_display_name VARCHAR,
    match_method VARCHAR NOT NULL,
    match_status VARCHAR NOT NULL CHECK (match_status IN ('resolved', 'ambiguous', 'unmatched')),
    source_file VARCHAR NOT NULL,
    source_row_number UBIGINT,
    PRIMARY KEY (provider, external_account_id)
);

CREATE TABLE performances_2024 (
    performance_id VARCHAR PRIMARY KEY,
    athlete_id UUID NOT NULL REFERENCES athletes(athlete_id),
    discipline VARCHAR NOT NULL,
    discipline_group VARCHAR NOT NULL,
    mark_text VARCHAR NOT NULL,
    mark_seconds DOUBLE CHECK (mark_seconds IS NULL OR mark_seconds > 0),
    mark_meters DOUBLE CHECK (mark_meters IS NULL OR mark_meters > 0),
    results_score INTEGER CHECK (results_score IS NULL OR results_score > 0),
    performance_date DATE NOT NULL CHECK (
        performance_date >= DATE '2024-01-01'
        AND performance_date < DATE '2025-01-01'
    ),
    location VARCHAR,
    nationality_code VARCHAR CHECK (nationality_code IS NULL OR length(nationality_code) = 3),
    gender VARCHAR NOT NULL CHECK (gender IN ('female', 'male', 'other', 'unknown')),
    is_season_best BOOLEAN NOT NULL,
    is_primary_discipline BOOLEAN NOT NULL,
    source_file VARCHAR NOT NULL,
    source_row_number UBIGINT NOT NULL,
    UNIQUE (athlete_id, discipline, mark_text, performance_date, location)
);

CREATE TABLE activities_2024 (
    activity_id VARCHAR PRIMARY KEY,
    athlete_id UUID NOT NULL REFERENCES athletes(athlete_id),
    provider VARCHAR NOT NULL CHECK (provider = 'strava'),
    external_account_id VARCHAR NOT NULL,
    provider_activity_type VARCHAR NOT NULL,
    activity_category VARCHAR NOT NULL CHECK (activity_category IN ('Run', 'Ride', 'Swim', 'Other')),
    activity_name VARCHAR,
    description VARCHAR,
    start_at_utc TIMESTAMPTZ NOT NULL CHECK (
        start_at_utc >= TIMESTAMPTZ '2024-01-01T00:00:00Z'
        AND start_at_utc < TIMESTAMPTZ '2025-01-01T00:00:00Z'
    ),
    week_start_utc DATE NOT NULL,
    distance_meters DOUBLE CHECK (distance_meters IS NULL OR distance_meters >= 0),
    elapsed_seconds DOUBLE CHECK (elapsed_seconds IS NULL OR elapsed_seconds >= 0),
    moving_seconds DOUBLE CHECK (moving_seconds IS NULL OR moving_seconds >= 0),
    pace_seconds_per_kilometer DOUBLE CHECK (
        pace_seconds_per_kilometer IS NULL OR pace_seconds_per_kilometer > 0
    ),
    location VARCHAR,
    quality_status VARCHAR NOT NULL CHECK (quality_status IN ('valid', 'warning')),
    quality_flags VARCHAR[] NOT NULL,
    exclusion_reason VARCHAR,
    source_file VARCHAR NOT NULL,
    source_row_number UBIGINT NOT NULL,
    FOREIGN KEY (provider, external_account_id)
        REFERENCES athlete_external_accounts(provider, external_account_id)
);

CREATE TABLE data_coverage_2024 (
    athlete_id UUID NOT NULL REFERENCES athletes(athlete_id),
    week_start_utc DATE NOT NULL,
    observation_status VARCHAR NOT NULL CHECK (
        observation_status IN ('observed', 'missing', 'unknown')
    ),
    is_active_week BOOLEAN NOT NULL,
    is_complete_enough_week BOOLEAN NOT NULL,
    collection_completed_at_utc TIMESTAMPTZ,
    collection_error_code VARCHAR,
    evidence_source VARCHAR,
    PRIMARY KEY (athlete_id, week_start_utc)
);

CREATE TABLE weekly_training_2024 (
    athlete_id UUID NOT NULL REFERENCES athletes(athlete_id),
    week_start_utc DATE NOT NULL,
    observation_status VARCHAR NOT NULL CHECK (
        observation_status IN ('observed', 'missing', 'unknown')
    ),
    activity_count UINTEGER NOT NULL,
    active_days UINTEGER NOT NULL CHECK (active_days <= 7),
    run_count UINTEGER NOT NULL,
    run_distance_meters DOUBLE NOT NULL CHECK (run_distance_meters >= 0),
    run_duration_seconds DOUBLE NOT NULL CHECK (run_duration_seconds >= 0),
    longest_run_meters DOUBLE CHECK (longest_run_meters IS NULL OR longest_run_meters >= 0),
    ride_count UINTEGER NOT NULL,
    ride_distance_meters DOUBLE NOT NULL CHECK (ride_distance_meters >= 0),
    ride_duration_seconds DOUBLE NOT NULL CHECK (ride_duration_seconds >= 0),
    swim_count UINTEGER NOT NULL,
    swim_distance_meters DOUBLE CHECK (swim_distance_meters IS NULL OR swim_distance_meters >= 0),
    swim_duration_seconds DOUBLE NOT NULL CHECK (swim_duration_seconds >= 0),
    other_count UINTEGER NOT NULL,
    other_duration_seconds DOUBLE NOT NULL CHECK (other_duration_seconds >= 0),
    PRIMARY KEY (athlete_id, week_start_utc),
    FOREIGN KEY (athlete_id, week_start_utc)
        REFERENCES data_coverage_2024(athlete_id, week_start_utc)
);

CREATE TABLE athlete_summary_2024 (
    athlete_id UUID PRIMARY KEY REFERENCES athletes(athlete_id),
    coverage_status VARCHAR NOT NULL CHECK (
        coverage_status IN ('complete', 'partial', 'insufficient', 'unknown')
    ),
    observed_weeks UINTEGER NOT NULL,
    active_weeks UINTEGER NOT NULL,
    complete_enough_weeks UINTEGER NOT NULL,
    total_activity_count UINTEGER NOT NULL,
    total_run_distance_meters DOUBLE NOT NULL CHECK (total_run_distance_meters >= 0),
    total_run_duration_seconds DOUBLE NOT NULL CHECK (total_run_duration_seconds >= 0),
    average_run_distance_per_observed_week_meters DOUBLE,
    average_run_duration_per_observed_week_seconds DOUBLE,
    median_weekly_run_distance_meters DOUBLE,
    weekly_run_distance_stddev_meters DOUBLE,
    longest_run_meters DOUBLE,
    computed_at_utc TIMESTAMPTZ NOT NULL
);

CREATE TABLE quarantined_records (
    quarantine_id VARCHAR PRIMARY KEY,
    build_id UUID NOT NULL REFERENCES dataset_builds(build_id),
    entity_type VARCHAR NOT NULL CHECK (
        entity_type IN ('athlete', 'external_account', 'performance', 'activity', 'coverage')
    ),
    source_file VARCHAR NOT NULL,
    source_row_number UBIGINT,
    source_record_json JSON NOT NULL,
    reason_code VARCHAR NOT NULL,
    reason_detail VARCHAR,
    quarantined_at_utc TIMESTAMPTZ NOT NULL,
    UNIQUE (build_id, entity_type, source_file, source_row_number, reason_code)
);
