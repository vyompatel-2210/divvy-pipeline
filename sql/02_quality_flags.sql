-- One row per (record, rule failed). A record can fail several rules.
CREATE OR REPLACE TABLE dq_flags AS
WITH parsed AS (
    SELECT
        source_file,
        source_row,
        NULLIF(TRIM(ride_id), '')    AS rid,
        NULLIF(TRIM(started_at), '') AS s_txt,
        NULLIF(TRIM(ended_at), '')   AS e_txt,
        TRY_CAST(NULLIF(TRIM(started_at), '') AS TIMESTAMP) AS s_ts,
        TRY_CAST(NULLIF(TRIM(ended_at), '')   AS TIMESTAMP) AS e_ts
    FROM raw_trips
)
SELECT source_file, source_row, rid AS ride_id, 'missing_ride_id' AS rule
FROM parsed WHERE rid IS NULL
UNION ALL
SELECT source_file, source_row, rid, 'missing_started_at'
FROM parsed WHERE s_txt IS NULL
UNION ALL
SELECT source_file, source_row, rid, 'missing_ended_at'
FROM parsed WHERE e_txt IS NULL
UNION ALL
SELECT source_file, source_row, rid, 'unparseable_started_at'
FROM parsed WHERE s_txt IS NOT NULL AND s_ts IS NULL
UNION ALL
SELECT source_file, source_row, rid, 'unparseable_ended_at'
FROM parsed WHERE e_txt IS NOT NULL AND e_ts IS NULL
UNION ALL
SELECT source_file, source_row, rid, 'ends_before_start'
FROM parsed WHERE s_ts IS NOT NULL AND e_ts IS NOT NULL AND e_ts < s_ts;

-- Duplicate ride IDs, tracked separately from flags.
-- 'within_file'   : repeated inside a single input file
-- 'across_files'  : the same ride shows up in more than one input file
-- (A repeated *load* cannot create duplicates: see repeat_load_rows in dq_report.)
CREATE OR REPLACE TABLE dq_duplicates AS
SELECT
    NULLIF(TRIM(ride_id), '')                                AS ride_id,
    count(*)                                                 AS copies,
    count(DISTINCT source_file)                              AS distinct_files,
    CASE WHEN count(DISTINCT source_file) > 1
         THEN 'across_files' ELSE 'within_file' END          AS duplicate_origin,
    array_to_string(list_sort(list(DISTINCT source_file)), ', ') AS files
FROM raw_trips
WHERE NULLIF(TRIM(ride_id), '') IS NOT NULL
GROUP BY NULLIF(TRIM(ride_id), '')
HAVING count(*) > 1;
