"""Tests run the full pipeline on data/sample, which contains deliberately bad records.

Sample layout (see data/sample/*.csv):
  sample_202401.csv: 14 rows   sample_202402.csv: 7 rows   -> 21 loaded
  8 records flagged (9 rule hits: one record fails two rules)
  2 duplicates removed (1 within a file, 1 across files)
  11 clean trips (1 with a missing start station)
"""
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from divvy_pipeline import build_warehouse, load_csvs, run_pipeline

SAMPLE = Path(__file__).resolve().parents[1] / "data" / "sample"


@pytest.fixture()
def con():
    connection = duckdb.connect(":memory:")
    load_csvs(connection, SAMPLE)
    build_warehouse(connection)
    yield connection
    connection.close()


def scalar(con, sql, params=None):
    return con.execute(sql, params or []).fetchone()[0]


def report(con):
    return {
        (m, r): n
        for m, r, n in con.execute("SELECT metric, rule, row_count FROM dq_report").fetchall()
    }


# ---- load -------------------------------------------------------------------
def test_raw_keeps_every_row_with_source_file(con):
    assert scalar(con, "SELECT count(*) FROM raw_trips") == 21
    files = dict(con.execute(
        "SELECT source_file, count(*) FROM raw_trips GROUP BY 1").fetchall())
    assert files == {"sample_202401.csv": 14, "sample_202402.csv": 7}


def test_raw_values_are_untouched_strings(con):
    row = con.execute(
        "SELECT started_at, ended_at FROM raw_trips WHERE ride_id = 'BADTS002'").fetchone()
    assert row == ("2024-01-03 13:00:00", "2024-13-45 99:00:00")


def test_loading_twice_does_not_change_counts():
    con = duckdb.connect(":memory:")
    first = load_csvs(con, SAMPLE)
    build_warehouse(con)
    before = (scalar(con, "SELECT count(*) FROM raw_trips"),
              scalar(con, "SELECT count(*) FROM clean_trips"),
              report(con))

    second = load_csvs(con, SAMPLE)
    build_warehouse(con)
    after = (scalar(con, "SELECT count(*) FROM raw_trips"),
             scalar(con, "SELECT count(*) FROM clean_trips"),
             report(con))

    assert all(r["status"] == "loaded" for r in first)
    assert all(r["status"].startswith("skipped") for r in second)
    assert before == after
    assert report(con)[("repeat_load_rows", "")] == 0


def test_changed_file_is_replaced_not_appended(tmp_path):
    for f in SAMPLE.glob("*.csv"):
        (tmp_path / f.name).write_text(f.read_text())
    con = duckdb.connect(":memory:")
    load_csvs(con, tmp_path)
    jan = tmp_path / "sample_202401.csv"
    lines = jan.read_text().splitlines()
    jan.write_text("\n".join(lines[:-1]) + "\n")  # drop the last row
    result = load_csvs(con, tmp_path)
    assert {r["file"]: r["status"] for r in result}["sample_202401.csv"].startswith("reloaded")
    assert scalar(con, "SELECT count(*) FROM raw_trips") == 20


# ---- data-quality rules ------------------------------------------------------
@pytest.mark.parametrize("rule,expected_ids", [
    ("missing_ride_id",        {"", None}),
    ("missing_started_at",     {"NOSTART1"}),
    ("missing_ended_at",       {"NOEND001"}),
    ("unparseable_started_at", {"BADTS001", None}),
    ("unparseable_ended_at",   {"BADTS002"}),
    ("ends_before_start",      {"NEG00001", "NEG00002"}),
])
def test_each_rule_flags_the_right_records(con, rule, expected_ids):
    ids = [r[0] for r in con.execute(
        "SELECT ride_id FROM dq_flags WHERE rule = ?", [rule]).fetchall()]
    assert len(ids) == (2 if rule in ("missing_ride_id", "unparseable_started_at",
                                      "ends_before_start") else 1)
    assert set(ids) <= expected_ids


def test_record_failing_two_rules_appears_twice_in_flags(con):
    rules = {r[0] for r in con.execute(
        "SELECT rule FROM dq_flags WHERE source_file='sample_202401.csv' AND source_row=14"
    ).fetchall()}
    assert rules == {"missing_ride_id", "unparseable_started_at"}


def test_flagged_records_are_excluded_from_clean_trips(con):
    bad = {"NOSTART1", "NOEND001", "BADTS001", "BADTS002", "NEG00001", "NEG00002"}
    present = {r[0] for r in con.execute("SELECT ride_id FROM clean_trips").fetchall()}
    assert not (bad & present)


def test_duplicates_are_tracked_by_origin(con):
    dups = {r[0]: r[1] for r in con.execute(
        "SELECT ride_id, duplicate_origin FROM dq_duplicates").fetchall()}
    assert dups == {"DUPSAME1": "within_file", "GOOD003": "across_files"}
    removed = con.execute(
        "SELECT ride_id, kept_source_file, identical_to_kept FROM dq_removed_duplicates "
        "ORDER BY ride_id").fetchall()
    assert removed == [("DUPSAME1", "sample_202401.csv", True),
                       ("GOOD003", "sample_202401.csv", True)]


def test_report_reconciles(con):
    r = report(con)
    assert r[("rows_loaded", "")] == 21
    assert r[("rows_flagged", "")] == 8
    assert r[("duplicates_removed", "")] == 2
    assert r[("rows_kept", "")] == 11
    assert r[("unaccounted_rows", "")] == 0
    assert r[("kept_missing_start_station", "")] == 1
    assert r[("flagged_by_rule", "missing_ride_id")] == 2
    assert r[("duplicate_extra_copies_by_origin", "within_file")] == 1
    assert r[("duplicate_extra_copies_by_origin", "across_files")] == 1


# ---- clean_trips -------------------------------------------------------------
def test_clean_trips_one_row_per_ride_id(con):
    assert scalar(con, "SELECT count(*) FROM clean_trips") == \
           scalar(con, "SELECT count(DISTINCT ride_id) FROM clean_trips") == 11


def test_derived_columns(con):
    row = con.execute(
        "SELECT duration_min, trip_month, day_of_week, day_name, is_weekend, start_hour "
        "FROM clean_trips WHERE ride_id = 'GOOD003'").fetchone()
    assert row == (30.0, date(2024, 1, 1), 6, "Saturday", True, 10)
    assert scalar(con, "SELECT duration_min FROM clean_trips WHERE ride_id='GOOD001'") == 15.0


def test_missing_station_trip_counts_citywide_but_not_in_rankings(con):
    assert scalar(con, "SELECT count(*) FROM clean_trips WHERE ride_id='NOSTN01'") == 1
    assert scalar(con, "SELECT sum(departures) FROM hourly_departures WHERE day_type='all'") == 11
    assert scalar(con, "SELECT sum(departures) FROM station_rank_monthly") == 10
    names = {r[0] for r in con.execute(
        "SELECT start_station_name FROM station_rank_monthly").fetchall()}
    assert None not in names and "" not in names


# ---- analysis ----------------------------------------------------------------
def test_station_rank_within_month_uses_window_and_tiebreak(con):
    rows = con.execute(
        "SELECT trip_month, rank_in_month, start_station_name, departures "
        "FROM station_top10_monthly ORDER BY trip_month, rank_in_month").fetchall()
    assert rows == [
        (date(2024, 1, 1), 1, "Clark St & Elm St", 3),
        (date(2024, 1, 1), 2, "Streeter Dr & Grand Ave", 2),
        (date(2024, 2, 1), 1, "Clark St & Elm St", 3),
        (date(2024, 2, 1), 2, "Streeter Dr & Grand Ave", 1),   # tie with Wells; name breaks it
        (date(2024, 2, 1), 3, "Wells St & Concord Ln", 1),
    ]


def test_hourly_has_all_24_hours_and_averages_per_day(con):
    for day_type in ("all", "weekday", "weekend"):
        assert scalar(con, "SELECT count(*) FROM hourly_departures WHERE day_type=?",
                      [day_type]) == 24
    # 4 weekday departures in hour 8 over 29 weekdays
    assert scalar(con, "SELECT departures FROM hourly_departures "
                       "WHERE day_type='weekday' AND start_hour=8") == 4
    assert scalar(con, "SELECT avg_departures_per_day FROM hourly_departures "
                       "WHERE day_type='weekday' AND start_hour=8") == pytest.approx(4 / 29, abs=1e-3)


def test_weekday_weekend_uses_average_per_day_not_raw_counts(con):
    first, last = date(2024, 1, 1), date(2024, 2, 8)   # first and last trip dates in sample
    days = [first + timedelta(d) for d in range((last - first).days + 1)]
    n_weekend = sum(d.weekday() >= 5 for d in days)
    n_weekday = len(days) - n_weekend

    rows = {r[0]: r for r in con.execute(
        "SELECT day_type, n_days, trips, avg_trips_per_day FROM weekday_weekend_summary").fetchall()}
    assert rows["weekday"][1:3] == (n_weekday, 8)
    assert rows["weekend"][1:3] == (n_weekend, 3)
    assert rows["weekday"][3] == pytest.approx(8 / n_weekday, abs=0.05)
    assert rows["weekend"][3] == pytest.approx(3 / n_weekend, abs=0.05)


# ---- end to end --------------------------------------------------------------
def test_full_run_writes_outputs_and_chart(tmp_path):
    result = run_pipeline(SAMPLE, tmp_path / "w.duckdb", tmp_path / "out")
    names = {p.name for p in result["exports"]}
    assert {"dq_report.csv", "dq_flagged_records.csv", "station_top10_monthly.csv"} <= names
    assert result["chart"].exists() and result["chart"].stat().st_size > 0
    flagged = (tmp_path / "out" / "dq_flagged_records.csv").read_text()
    assert "ends_before_start" in flagged and "unparseable_ended_at" in flagged
