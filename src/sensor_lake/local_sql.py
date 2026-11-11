"""Run the Athena queries locally with DuckDB against the curated Parquet.

Athena is Trino under the hood; the queries in athena/queries/ only use SQL
that both Trino and DuckDB understand, so running them here verifies their
logic and results without an AWS account. Tables are exposed under the
same names as in Athena (sensor_lake.<table>).

    python -m sensor_lake.local_sql            # runs all, writes sample_output/athena/
"""
from __future__ import annotations

import time
from pathlib import Path

import duckdb

from .config import PROJECT_ROOT, LakePaths, default_lake_root

TABLES = {
    "hourly_machine_metrics": "curated_hourly", "daily_machine_performance": "curated_daily",
    "anomalies": "curated_anomalies", "alerts": "curated_alerts",
}


def connect(lake_root: str | None = None) -> duckdb.DuckDBPyConnection:
    p = LakePaths(lake_root or default_lake_root())
    con = duckdb.connect()
    con.execute("CREATE SCHEMA sensor_lake")
    for table, attr in TABLES.items():
        path = getattr(p, attr)
        con.execute(f"CREATE VIEW sensor_lake.{table} AS SELECT * FROM read_parquet("
                    f"'{path}/**/*.parquet', hive_partitioning = true)")
    con.execute(f"CREATE VIEW sensor_lake.dim_machine AS SELECT * FROM "
                f"read_parquet('{p.curated_machines}/*.parquet')")
    return con


def main() -> None:
    con = connect()
    out = PROJECT_ROOT / "sample_output" / "athena"
    out.mkdir(parents=True, exist_ok=True)
    report = ["# Athena queries executed locally with DuckDB\n"]
    for f in sorted((PROJECT_ROOT / "athena" / "queries").glob("*.sql")):
        t0 = time.perf_counter()
        df = con.execute(f.read_text(encoding="utf-8")).fetchdf()
        ms = (time.perf_counter() - t0) * 1000
        df.to_csv(out / f"{f.stem}.csv", index=False)
        print(f"{f.name:45s} {len(df):4d} rows {ms:7.0f} ms")
        report += [f"\n## {f.stem}\n", f"{len(df)} rows, {ms:.0f} ms\n",
                   df.head(15).to_string(index=False).join(["```\n", "\n```"])]
    (PROJECT_ROOT / "sample_output" / "athena_results.md").write_text("\n".join(report),
                                                                      encoding="utf-8")


if __name__ == "__main__":
    main()
