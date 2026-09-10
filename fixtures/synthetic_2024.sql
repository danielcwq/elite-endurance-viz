-- Entirely invented examples; NOT sampled, masked, or derived from athlete data.
-- All source IDs are deliberately nonnumeric placeholders, not real Strava IDs.
CREATE TEMP TABLE personas AS
SELECT ('00000000-0000-4000-8000-' || lpad(n::VARCHAR,12,'0'))::UUID AS athlete_id,
       n, name, gender, country, event
FROM (VALUES
    (1,'SYNTHETIC Frequent','female','CAN','5000m'),
    (2,'SYNTHETIC Missing Distance','male','GBR','800m'),
    (3,'SYNTHETIC Cross Training','female','AUS','1500m'),
    (4,'SYNTHETIC No Observations','unknown',NULL,NULL),
    (5,'SYNTHETIC Split Efforts','male','CAN','3000m Steeplechase')
) t(n,name,gender,country,event);

INSERT INTO athletes
SELECT athlete_id,name,lower(name),name,country,gender,'resolved',
       'synthetic-demo-not-real',TIMESTAMPTZ '2025-01-01T00:00:00Z',
       TIMESTAMPTZ '2025-01-01T00:00:00Z' FROM personas;

INSERT INTO athlete_external_accounts
SELECT 'strava','synthetic-account-' || n,athlete_id,name,'synthetic-fixture',
       'resolved','fixtures/synthetic_2024.sql',n FROM personas WHERE n<>4;

INSERT INTO performances_2024
SELECT 'synthetic-performance-' || n,athlete_id,event,'synthetic',
       CASE n WHEN 1 THEN '15:00.00' WHEN 2 THEN '1:48.00' WHEN 3 THEN '4:20.00' ELSE '8:30.00' END,
       CASE n WHEN 1 THEN 900 WHEN 2 THEN 108 WHEN 3 THEN 260 ELSE 510 END,
       NULL,CASE WHEN n=3 THEN NULL ELSE 1100+n*10 END,DATE '2024-07-01',
       'Fictional venue',country,gender,true,true,'fixtures/synthetic_2024.sql',n
FROM personas WHERE event IS NOT NULL;

CREATE TEMP TABLE demo_activity_rows AS
-- Eight weeks with five records each: exercises both pages of the activity table.
SELECT 1 AS persona, 'frequent-' || i AS record_id,
       TIMESTAMPTZ '2024-01-01T08:00:00Z' + (i//5*7 + i%5)*INTERVAL '1 day' AS started,
       'Run' AS category,'Run' AS original_type,6000.0+i%5*500 AS distance,1800.0 AS duration
FROM range(40) t(i)
UNION ALL
SELECT * FROM (VALUES
    (2,'missing-distance',TIMESTAMPTZ '2024-01-01T08:00:00Z','Run','Run',NULL,600),
    (2,'known-distance',TIMESTAMPTZ '2024-01-02T08:00:00Z','Run','Run',5000,1500),
    (2,'real-zero',TIMESTAMPTZ '2024-01-08T08:00:00Z','Run','Run',0,30),
    (2,'partial-week',TIMESTAMPTZ '2024-12-30T08:00:00Z','Run','TrailRun',9000,2700),
    (3,'ride',TIMESTAMPTZ '2024-01-01T08:00:00Z','Ride','VirtualRide',20000,2400),
    (3,'swim',TIMESTAMPTZ '2024-01-02T08:00:00Z','Swim','Swim',NULL,1800),
    (3,'strength',TIMESTAMPTZ '2024-01-03T08:00:00Z','Other','WeightTraining',NULL,1200)
) t(persona,record_id,started,category,original_type,distance,duration)
UNION ALL
-- Eight short efforts on the same day are eight records, not eight sessions.
SELECT 5,'split-' || i,TIMESTAMPTZ '2024-02-05T08:00:00Z'+i*INTERVAL '2 minutes',
       'Run','Run',100,20 FROM range(8) t(i);

INSERT INTO activities_2024
SELECT 'synthetic-activity-' || r.record_id,p.athlete_id,'strava','synthetic-account-' || p.n,
       original_type,category,'SYNTHETIC ' || record_id,NULL,started,
       date_trunc('week',started AT TIME ZONE 'UTC')::DATE,distance,duration,duration,
       CASE WHEN category='Run' AND distance>0 THEN duration/(distance/1000) ELSE NULL END,
       NULL,CASE WHEN distance IS NULL THEN 'warning' ELSE 'valid' END,
       CASE WHEN distance IS NULL THEN ['SYNTHETIC_MISSING_DISTANCE'] ELSE [] END,
       NULL,'fixtures/synthetic_2024.sql',row_number() OVER(ORDER BY persona,started,record_id)
FROM demo_activity_rows r JOIN personas p ON r.persona=p.n;

INSERT INTO data_coverage_2024
WITH weeks AS (
    SELECT p.athlete_id,p.n,w::DATE AS week_start,
           w=DATE '2024-12-30' AS partial,
           count(a.activity_id) AS records
    FROM personas p CROSS JOIN
         generate_series(DATE '2024-01-01',DATE '2024-12-30',INTERVAL '1 week') t(w)
    LEFT JOIN activities_2024 a ON a.athlete_id=p.athlete_id AND a.week_start_utc=w::DATE
    GROUP BY p.athlete_id,p.n,w
), states AS (
    SELECT *, CASE
        WHEN n=4 THEN 'unknown'
        WHEN n=5 THEN 'missing'
        WHEN records>0 OR (n=1 AND week_start=DATE '2024-03-04') THEN 'observed'
        ELSE 'missing' END AS observation,
        n=2 AND week_start=DATE '2024-01-01' AS warning
    FROM weeks
)
SELECT athlete_id,week_start,observation,records>0,
       observation='observed' AND NOT partial AND NOT warning,partial,records,
       CASE WHEN partial THEN 'partial_window' WHEN warning THEN 'observed_with_warning'
            WHEN observation='observed' THEN 'complete' ELSE observation END,
       NULL,CASE WHEN warning THEN 'SYNTHETIC_SOURCE_CONFLICT' ELSE NULL END,
       CASE WHEN observation='observed' THEN 'fixtures/synthetic_2024.sql' ELSE NULL END,
       CASE WHEN observation='observed' THEN 1 ELSE 0 END
FROM states;
