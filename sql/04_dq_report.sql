-- Small, human-readable reconciliation:
--   rows_loaded = rows_flagged + duplicates_removed + rows_kept
CREATE OR REPLACE TABLE dq_report AS
WITH
loaded  AS (SELECT count(*) AS n FROM raw_trips),
flagged AS (SELECT count(*) AS n FROM (SELECT DISTINCT source_file, source_row FROM dq_flags)),
removed AS (SELECT count(*) AS n FROM dq_removed_duplicates),
kept    AS (SELECT count(*) AS n FROM clean_trips),
rules(rule, note) AS (
    VALUES
    ('missing_ride_id',        'ride_id is NULL or blank'),
    ('missing_started_at',     'started_at is NULL or blank'),
    ('missing_ended_at',       'ended_at is NULL or blank'),
    ('unparseable_started_at', 'started_at present but not a valid timestamp'),
    ('unparseable_ended_at',   'ended_at present but not a valid timestamp'),
    ('ends_before_start',      'ended_at is earlier than started_at')
),
by_rule AS (SELECT rule, count(*) AS n FROM dq_flags GROUP BY rule),
origins(origin) AS (VALUES ('within_file'), ('across_files')),
dup_extra AS (
    SELECT duplicate_origin AS origin, SUM(copies - 1) AS n
    FROM dq_duplicates GROUP BY duplicate_origin
)
SELECT 10 AS ord, 'rows_loaded' AS metric, '' AS rule, n::BIGINT AS row_count,
       'All rows in raw_trips' AS note
FROM loaded
UNION ALL
SELECT 20, 'rows_flagged', '', n::BIGINT,
       'Distinct records failing at least one rule (excluded from clean_trips)'
FROM flagged
UNION ALL
SELECT 21, 'flagged_by_rule', r.rule, coalesce(b.n, 0)::BIGINT,
       r.note || ' (a record can fail more than one rule)'
FROM rules r LEFT JOIN by_rule b ON b.rule = r.rule
UNION ALL
SELECT 30, 'duplicates_removed', '', n::BIGINT,
       'Extra copies of an already-kept ride_id dropped from clean_trips'
FROM removed
UNION ALL
SELECT 31, 'duplicate_extra_copies_by_origin', o.origin, coalesce(d.n, 0)::BIGINT,
       'Extra copies of a ride_id found in the raw input, by where they came from'
FROM origins o LEFT JOIN dup_extra d ON d.origin = o.origin
UNION ALL
SELECT 32, 'repeat_load_rows', '', n::BIGINT,
       'Rows whose (source_file, source_row) appears more than once; must be 0'
FROM (SELECT count(*) AS n FROM (
        SELECT source_file, source_row FROM raw_trips
        GROUP BY source_file, source_row HAVING count(*) > 1))
UNION ALL
SELECT 40, 'rows_kept', '', n::BIGINT, 'Rows in clean_trips (one per ride_id)'
FROM kept
UNION ALL
SELECT 41, 'kept_missing_start_station', '', count(*)::BIGINT,
       'Kept in citywide totals, excluded from station rankings'
FROM clean_trips WHERE NOT has_start_station
UNION ALL
SELECT 50, 'unaccounted_rows', '',
       (loaded.n - flagged.n - removed.n - kept.n)::BIGINT,
       'rows_loaded - flagged - duplicates_removed - kept; must be 0'
FROM loaded, flagged, removed, kept;
