-- Raw layer: every value stays exactly as it appeared in the CSV (all VARCHAR).
-- (source_file, source_row) is the trace-back key for any questionable record.
CREATE TABLE IF NOT EXISTS raw_trips (
    ride_id            VARCHAR,
    rideable_type      VARCHAR,
    started_at         VARCHAR,
    ended_at           VARCHAR,
    start_station_name VARCHAR,
    start_station_id   VARCHAR,
    end_station_name   VARCHAR,
    end_station_id     VARCHAR,
    start_lat          VARCHAR,
    start_lng          VARCHAR,
    end_lat            VARCHAR,
    end_lng            VARCHAR,
    member_casual      VARCHAR,
    source_file        VARCHAR,
    source_row         BIGINT,
    loaded_at          TIMESTAMP
);

-- One row per input file; used to make loads idempotent and auditable.
CREATE TABLE IF NOT EXISTS load_log (
    source_file VARCHAR,   -- logical key (no constraint: DuckDB checks PKs eagerly inside a transaction)
    sha256      VARCHAR,
    rows_loaded BIGINT,
    loaded_at   TIMESTAMP
);
