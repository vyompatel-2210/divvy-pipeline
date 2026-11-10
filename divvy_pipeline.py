#!/usr/bin/env python3
"""Chicago Divvy trips pipeline: CSVs -> DuckDB raw -> clean -> quality report -> analysis.

Usage:
    python divvy_pipeline.py --input data/raw --db warehouse/divvy.duckdb --out outputs
"""
from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent
SQL_DIR = ROOT / "sql"

EXPECTED_COLUMNS = [
    "ride_id", "rideable_type", "started_at", "ended_at",
    "start_station_name", "start_station_id", "end_station_name", "end_station_id",
    "start_lat", "start_lng", "end_lat", "end_lng", "member_casual",
]


# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------
def sql_literal(value: str | Path) -> str:
    """Quote a string for safe inline use in SQL."""
    return "'" + str(value).replace("'", "''") + "'"


def split_sql(text: str) -> list[str]:
    """Split SQL text into statements on ';', ignoring ';' inside '...' strings
    and dropping -- comments."""
    statements, current = [], []
    in_string = False
    i = 0
    while i < len(text):
        ch = text[i]
        if in_string:
            current.append(ch)
            if ch == "'":
                if text[i + 1:i + 2] == "'":      # escaped quote ''
                    current.append("'")
                    i += 1
                else:
                    in_string = False
        elif ch == "'":
            in_string = True
            current.append(ch)
        elif text.startswith("--", i):             # comment to end of line
            while i < len(text) and text[i] != "\n":
                i += 1
            continue
        elif ch == ";":
            statements.append("".join(current))
            current = []
        else:
            current.append(ch)
        i += 1
    statements.append("".join(current))
    return [st.strip() for st in statements if st.strip()]


def run_sql_file(con: duckdb.DuckDBPyConnection, name: str) -> None:
    """Execute every statement in sql/<name>."""
    text = (SQL_DIR / name).read_text(encoding="utf-8")
    for statement in split_sql(text):
        con.execute(statement)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def check_columns(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    described = con.execute(
        f"DESCRIBE SELECT * FROM read_csv({sql_literal(path)}, header=true, all_varchar=true)"
    ).fetchall()
    found = {row[0] for row in described}
    missing = [c for c in EXPECTED_COLUMNS if c not in found]
    if missing:
        raise ValueError(
            f"{path.name}: missing expected columns {missing}. "
            "This loader supports the Divvy schema used since December 2020."
        )


# ----------------------------------------------------------------------------
# step 1: load raw
# ----------------------------------------------------------------------------
def load_csvs(con: duckdb.DuckDBPyConnection, input_dir: Path) -> list[dict]:
    """Load every CSV in input_dir into raw_trips. Safe to re-run.

    Idempotency: each file is loaded atomically with delete-then-insert keyed on
    source_file, and unchanged files (same SHA-256) are skipped entirely. Running
    the loader twice on the same files therefore leaves raw_trips unchanged.
    """
    files = sorted(p for p in Path(input_dir).iterdir() if p.suffix.lower() == ".csv")
    if not files:
        raise FileNotFoundError(f"No .csv files found in {input_dir}")

    run_sql_file(con, "01_schema.sql")
    results = []
    cols = ", ".join(EXPECTED_COLUMNS)

    for path in files:
        digest = file_sha256(path)
        prev = con.execute(
            "SELECT sha256, rows_loaded FROM load_log WHERE source_file = ?", [path.name]
        ).fetchone()
        if prev and prev[0] == digest:
            results.append({"file": path.name, "status": "skipped (unchanged)", "rows": prev[1]})
            continue

        check_columns(con, path)
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute("DELETE FROM raw_trips WHERE source_file = ?", [path.name])
            con.execute(
                f"""
                INSERT INTO raw_trips ({cols}, source_file, source_row, loaded_at)
                SELECT {cols},
                       ? AS source_file,
                       row_number() OVER () AS source_row,
                       now() AS loaded_at
                FROM read_csv({sql_literal(path)}, header=true, all_varchar=true)
                """,
                [path.name],
            )
            rows = con.execute(
                "SELECT count(*) FROM raw_trips WHERE source_file = ?", [path.name]
            ).fetchone()[0]
            con.execute("DELETE FROM load_log WHERE source_file = ?", [path.name])
            con.execute(
                "INSERT INTO load_log VALUES (?, ?, ?, now())", [path.name, digest, rows]
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        status = "reloaded (changed)" if prev else "loaded"
        results.append({"file": path.name, "status": status, "rows": rows})
    return results


# ----------------------------------------------------------------------------
# steps 2-4: transform
# ----------------------------------------------------------------------------
def build_warehouse(con: duckdb.DuckDBPyConnection) -> None:
    run_sql_file(con, "02_quality_flags.sql")
    run_sql_file(con, "03_clean_trips.sql")
    run_sql_file(con, "04_dq_report.sql")
    run_sql_file(con, "05_analysis.sql")


# ----------------------------------------------------------------------------
# outputs
# ----------------------------------------------------------------------------
EXPORTS = {
    "dq_report.csv":
        "SELECT metric, rule, row_count, note FROM dq_report ORDER BY ord, rule",
    "dq_flagged_records.csv":
        "SELECT source_file, source_row, ride_id, rule FROM dq_flags "
        "ORDER BY source_file, source_row, rule",
    "dq_duplicates.csv":
        "SELECT ride_id, copies, duplicate_origin, files FROM dq_duplicates ORDER BY ride_id",
    "dq_removed_duplicates.csv":
        "SELECT * FROM dq_removed_duplicates ORDER BY ride_id, source_file, source_row",
    "station_top10_monthly.csv":
        "SELECT * FROM station_top10_monthly ORDER BY trip_month, rank_in_month",
    "hourly_departures.csv":
        "SELECT * FROM hourly_departures ORDER BY day_type, start_hour",
    "weekday_weekend_summary.csv":
        "SELECT * FROM weekday_weekend_summary ORDER BY day_type",
}


def export_tables(con: duckdb.DuckDBPyConnection, out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for filename, query in EXPORTS.items():
        target = out_dir / filename
        con.execute(f"COPY ({query}) TO {sql_literal(target)} (HEADER, DELIMITER ',')")
        written.append(target)
    return written


def make_chart(con: duckdb.DuckDBPyConnection, out_dir: Path) -> Path:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    rows = con.execute(
        "SELECT day_type, start_hour, avg_departures_per_day FROM hourly_departures "
        "ORDER BY day_type, start_hour"
    ).fetchall()
    span = con.execute("SELECT min(trip_date), max(trip_date) FROM clean_trips").fetchone()

    series: dict[str, tuple[list[int], list[float]]] = {}
    for day_type, hour, avg in rows:
        xs, ys = series.setdefault(day_type, ([], []))
        xs.append(hour)
        ys.append(float(avg))

    style = {"all": ("All days", "black", "--"), "weekday": ("Weekdays", "#1f77b4", "-"),
             "weekend": ("Weekends", "#d62728", "-")}
    fig, ax = plt.subplots(figsize=(9, 5))
    for key in ("weekday", "weekend", "all"):
        if key in series:
            label, color, ls = style[key]
            ax.plot(*series[key], label=label, color=color, linestyle=ls, linewidth=2)
    ax.set_xticks(range(0, 24, 2))
    ax.set_xlabel("Hour of day (trip start, local time)")
    ax.set_ylabel("Average departures per day")
    ax.set_title(f"Divvy: average departures by hour ({span[0]} to {span[1]})")
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / "avg_departures_by_hour.png"
    fig.savefig(target, dpi=150)
    plt.close(fig)
    return target


def print_report(con: duckdb.DuckDBPyConnection) -> None:
    print("\n=== Data-quality report ===")
    for metric, rule, n, note in con.execute(
        "SELECT metric, rule, row_count, note FROM dq_report ORDER BY ord, rule"
    ).fetchall():
        label = f"{metric}[{rule}]" if rule else metric
        print(f"{label:<55}{n:>12,}  {note}")


# ----------------------------------------------------------------------------
# orchestration
# ----------------------------------------------------------------------------
def run_pipeline(input_dir: Path, db_path: Path, out_dir: Path, chart: bool = True) -> dict:
    db_path = Path(db_path)
    if str(db_path) != ":memory:":
        db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        load_results = load_csvs(con, Path(input_dir))
        build_warehouse(con)
        files = export_tables(con, Path(out_dir))
        chart_path = make_chart(con, Path(out_dir)) if chart else None
        return {"loads": load_results, "exports": files, "chart": chart_path}
    finally:
        con.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input", default="data/raw", type=Path, help="folder of Divvy CSVs")
    parser.add_argument("--db", default="warehouse/divvy.duckdb", type=Path)
    parser.add_argument("--out", default="outputs", type=Path)
    parser.add_argument("--no-chart", action="store_true")
    args = parser.parse_args(argv)

    try:
        result = run_pipeline(args.input, args.db, args.out, chart=not args.no_chart)
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print("=== Load ===")
    for item in result["loads"]:
        print(f"{item['file']:<40}{item['status']:<22}{item['rows']:>12,} rows")

    con = duckdb.connect(str(args.db), read_only=True)
    try:
        print_report(con)
        print("\n=== Weekday vs weekend ===")
        for row in con.execute("SELECT * FROM weekday_weekend_summary").fetchall():
            print(row)
    finally:
        con.close()

    print(f"\nWrote {len(result['exports'])} tables to {args.out}/"
          + (f" and {result['chart'].name}" if result["chart"] else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
