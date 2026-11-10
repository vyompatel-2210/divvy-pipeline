-- Step 1: drop flagged records, rank the remaining copies of each ride_id.
-- The earliest (source_file, source_row) wins, so results are deterministic.
CREATE OR REPLACE TEMP TABLE stg_valid_ranked AS
SELECT
    TRIM(r.ride_id)    AS ride_id,
    r.rideable_type,
    TRIM(r.started_at) AS started_txt,
    TRIM(r.ended_at)   AS ended_txt,
    r.start_station_name, r.start_station_id,
    r.end_station_name,   r.end_station_id,
    r.start_lat, r.start_lng, r.end_lat, r.end_lng,
    r.member_casual,
    r.source_file,
    r.source_row,
    row_number() OVER (
        PARTITION BY TRIM(r.ride_id)
        ORDER BY r.source_file, r.source_row
    ) AS rn
FROM raw_trips r
LEFT JOIN dq_flags f
       ON f.source_file = r.source_file AND f.source_row = r.source_row
WHERE f.rule IS NULL;

-- Step 2: audit trail for every duplicate we dropped.
CREATE OR REPLACE TABLE dq_removed_duplicates AS
SELECT
    d.ride_id,
    d.source_file,
    d.source_row,
    k.source_file AS kept_source_file,
    k.source_row  AS kept_source_row,
    (    d.started_txt IS NOT DISTINCT FROM k.started_txt
     AND d.ended_txt   IS NOT DISTINCT FROM k.ended_txt
     AND d.start_station_name IS NOT DISTINCT FROM k.start_station_name
     AND d.end_station_name   IS NOT DISTINCT FROM k.end_station_name
    ) AS identical_to_kept
FROM stg_valid_ranked d
JOIN stg_valid_ranked k ON k.ride_id = d.ride_id AND k.rn = 1
WHERE d.rn > 1;

-- Step 3: one typed, enriched row per ride_id.
-- Valid trips with missing stations are KEPT (has_start_station = false).
CREATE OR REPLACE TABLE clean_trips AS
WITH typed AS (
    SELECT *,
           CAST(started_txt AS TIMESTAMP) AS started_at,
           CAST(ended_txt   AS TIMESTAMP) AS ended_at
    FROM stg_valid_ranked
    WHERE rn = 1
)
SELECT
    ride_id,
    rideable_type,
    member_casual,
    started_at,
    ended_at,
    ROUND(date_diff('second', started_at, ended_at) / 60.0, 2)  AS duration_min,
    CAST(started_at AS DATE)                                    AS trip_date,
    CAST(date_trunc('month', started_at) AS DATE)               AS trip_month,
    isodow(started_at)                                          AS day_of_week,   -- 1=Mon ... 7=Sun
    dayname(started_at)                                         AS day_name,
    isodow(started_at) >= 6                                     AS is_weekend,
    CASE WHEN isodow(started_at) >= 6 THEN 'weekend' ELSE 'weekday' END AS day_type,
    hour(started_at)                                            AS start_hour,
    NULLIF(TRIM(start_station_name), '')                        AS start_station_name,
    NULLIF(TRIM(start_station_id), '')                          AS start_station_id,
    NULLIF(TRIM(end_station_name), '')                          AS end_station_name,
    NULLIF(TRIM(end_station_id), '')                            AS end_station_id,
    NULLIF(TRIM(start_station_name), '') IS NOT NULL            AS has_start_station,
    TRY_CAST(start_lat AS DOUBLE)                               AS start_lat,
    TRY_CAST(start_lng AS DOUBLE)                               AS start_lng,
    TRY_CAST(end_lat AS DOUBLE)                                 AS end_lat,
    TRY_CAST(end_lng AS DOUBLE)                                 AS end_lng,
    source_file,
    source_row
FROM typed;
