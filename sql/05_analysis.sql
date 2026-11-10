-- Calendar of days covered by the data (first to last trip date).
-- Used as the denominator for "average per day", so days with zero trips still count.
CREATE OR REPLACE TABLE dim_calendar_days AS
SELECT
    s.lo + CAST(r.i AS INTEGER) AS trip_date,
    CASE WHEN isodow(s.lo + CAST(r.i AS INTEGER)) >= 6 THEN 'weekend' ELSE 'weekday' END AS day_type
FROM (SELECT min(trip_date) AS lo, max(trip_date) AS hi FROM clean_trips) s
CROSS JOIN range(0, 20000) AS r(i)
WHERE s.lo + CAST(r.i AS INTEGER) <= s.hi;

-- Q2: When does demand peak? Average departures per day, by hour.
-- day_type = 'all' | 'weekday' | 'weekend'. Every hour 0-23 is present (zeros included).
CREATE OR REPLACE TABLE hourly_departures AS
WITH day_counts AS (
    SELECT 'all' AS day_type, count(*) AS n_days FROM dim_calendar_days
    UNION ALL
    SELECT day_type, count(*) FROM dim_calendar_days GROUP BY day_type
),
buckets AS (
    SELECT h.start_hour, d.day_type, d.n_days
    FROM (SELECT CAST(x AS INTEGER) AS start_hour FROM range(0, 24) t(x)) h
    CROSS JOIN day_counts d
),
trips AS (
    SELECT 'all' AS day_type, start_hour, count(*) AS departures
    FROM clean_trips GROUP BY start_hour
    UNION ALL
    SELECT day_type, start_hour, count(*)
    FROM clean_trips GROUP BY day_type, start_hour
)
SELECT
    b.day_type,
    b.start_hour,
    coalesce(t.departures, 0)                                       AS departures,
    b.n_days,
    ROUND(coalesce(t.departures, 0) * 1.0 / b.n_days, 3)            AS avg_departures_per_day
FROM buckets b
LEFT JOIN trips t ON t.day_type = b.day_type AND t.start_hour = b.start_hour
ORDER BY b.day_type, b.start_hour;

-- Q3: Weekday vs weekend, normalised by the number of days of each type.
CREATE OR REPLACE TABLE weekday_weekend_summary AS
WITH days AS (
    SELECT day_type, count(*) AS n_days FROM dim_calendar_days GROUP BY day_type
),
agg AS (
    SELECT day_type,
           count(*)                                                 AS trips,
           ROUND(avg(duration_min), 1)                              AS avg_duration_min,
           ROUND(100.0 * avg(CASE WHEN member_casual = 'member' THEN 1 ELSE 0 END), 1) AS member_pct
    FROM clean_trips GROUP BY day_type
)
SELECT
    d.day_type,
    d.n_days,
    coalesce(a.trips, 0)                                            AS trips,
    ROUND(coalesce(a.trips, 0) * 1.0 / d.n_days, 1)                 AS avg_trips_per_day,
    a.avg_duration_min,
    a.member_pct,
    (SELECT arg_max(h.start_hour, h.avg_departures_per_day)
       FROM hourly_departures h WHERE h.day_type = d.day_type)      AS peak_hour
FROM days d
LEFT JOIN agg a ON a.day_type = d.day_type
ORDER BY d.day_type;

-- Q1: Which stations see the most departures? Ranked within each month with a window function.
-- Trips with no start station are excluded here (but remain in every citywide total above).
CREATE OR REPLACE TABLE station_rank_monthly AS
SELECT
    trip_month,
    start_station_name,
    count(*) AS departures,
    row_number() OVER (
        PARTITION BY trip_month
        ORDER BY count(*) DESC, start_station_name   -- name breaks ties deterministically
    ) AS rank_in_month
FROM clean_trips
WHERE has_start_station
GROUP BY trip_month, start_station_name;

CREATE OR REPLACE TABLE station_top10_monthly AS
SELECT trip_month, rank_in_month, start_station_name, departures
FROM station_rank_monthly
WHERE rank_in_month <= 10
ORDER BY trip_month, rank_in_month;
