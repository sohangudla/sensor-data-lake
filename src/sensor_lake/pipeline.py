"""Batch job: RAW -> CLEAN -> CURATED, then validate and publish metrics.

    python -m sensor_lake.pipeline                       # local lake ./lake
    python -m sensor_lake.pipeline --skip-clean          # rebuild curated only
    python -m sensor_lake.pipeline --lake-root s3://bucket/sensor-lake   (AWS)

Reruns are idempotent: every output is written with mode("overwrite") and
dynamic partition overwrite, so a rerun replaces the same partitions instead
of appending duplicates.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import clean as C
from . import curate as K
from .config import SENSORS, LakePaths, default_lake_root
from .monitoring import publish_metrics
from .schemas import MACHINE_SCHEMA
from .validate import raise_on_failure, validate_outputs

log = logging.getLogger("sensor_lake.pipeline")

CLOUDWATCH_METRICS = ["CleanRows", "DuplicatesRemoved", "CorruptRows", "AnomalyRows",
                      "AlertEvents", "ValidationFailures"]


class Timer:
    def __init__(self):
        self.t = time.perf_counter()
        self.stages: dict[str, float] = {}

    def lap(self, name: str) -> None:
        now = time.perf_counter()
        self.stages[name] = round(now - self.t, 1)
        log.info("stage %-10s done in %.1fs", name, self.stages[name])
        self.t = now


def clean_stage(spark: SparkSession, p: LakePaths) -> dict:
    """RAW JSON -> CLEAN Parquet (+ quarantine). Returns row counts."""
    raw = C.read_raw_readings(spark, p.raw_readings)
    # cache: parsed raw is used by several actions; without it Spark would
    # re-read and re-parse all the gzipped JSON for each one
    raw = raw.persist(StorageLevel.MEMORY_AND_DISK)
    n_raw = raw.count()
    good, corrupt = C.split_corrupt(raw)
    clean_df, bad_keys = C.clean_readings(good)
    n_corrupt, n_bad_keys = corrupt.count(), bad_keys.count()

    (corrupt.select(F.lit("corrupt_json").alias("reason"), F.col(C.CORRUPT_COL).alias("payload"),
                    "source_file", "ingest_date")
     .unionByName(bad_keys.select(F.lit("missing_key").alias("reason"),
                                  F.to_json(F.struct("reading_id", "machine_id")).alias("payload"),
                                  "source_file", "ingest_date"))
     .write.mode("overwrite").partitionBy("ingest_date").json(p.quarantine))

    (clean_df.repartition("event_date", "machine_id")        # -> one file per partition
     .write.mode("overwrite").partitionBy("event_date", "machine_id").parquet(p.clean_readings))
    raw.unpersist()
    return {"RawRows": n_raw, "CorruptRows": n_corrupt, "MissingKeyRows": n_bad_keys}


def curated_stage(spark: SparkSession, p: LakePaths, timer: Timer):
    # downstream always reads the PERSISTED clean layer (cheap Parquet, and
    # the same input a separate curated-only job would see)
    clean_df = spark.read.parquet(p.clean_readings)

    anomalies = K.detect_anomalies(clean_df)
    (anomalies.repartition("event_date").write.mode("overwrite")
     .partitionBy("event_date").parquet(p.curated_anomalies))
    anomalies = spark.read.parquet(p.curated_anomalies)
    alerts = K.build_alerts(anomalies)
    alerts.coalesce(1).write.mode("overwrite").partitionBy("event_date").parquet(p.curated_alerts)
    alerts = spark.read.parquet(p.curated_alerts)
    timer.lap("anomalies")

    hourly = K.hourly_metrics(clean_df, anomalies)
    (hourly.repartition("event_date").write.mode("overwrite")
     .partitionBy("event_date").parquet(p.curated_hourly))
    hourly = spark.read.parquet(p.curated_hourly)

    daily = K.daily_performance(hourly, alerts)
    daily.coalesce(1).write.mode("overwrite").partitionBy("event_date").parquet(p.curated_daily)
    daily = spark.read.parquet(p.curated_daily)

    machines = spark.read.schema(MACHINE_SCHEMA).json(p.raw_reference)
    machines.coalesce(1).write.mode("overwrite").parquet(p.curated_machines)
    timer.lap("curated")
    return clean_df, anomalies, alerts, hourly, daily


def run(spark: SparkSession, lake_root: str, publish: bool = True,
        skip_clean: bool = False) -> dict:
    p = LakePaths(lake_root)
    timer = Timer()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log.info("=== run %s | lake=%s | skip_clean=%s", run_id, lake_root, skip_clean)

    counts = {} if skip_clean else clean_stage(spark, p)
    timer.lap("clean")
    clean_df, anomalies, alerts, hourly, daily = curated_stage(spark, p, timer)

    n_clean = clean_df.count()
    stats = clean_df.agg(
        *[F.sum(F.col(f"{s}_invalid").cast("int")).alias(f"invalid_{s}") for s in SENSORS],
        *[F.sum(F.col(f"{s}_imputed").cast("int")).alias(f"imputed_{s}") for s in SENSORS],
        *[F.sum(F.col(s).isNull().cast("int")).alias(f"still_null_{s}") for s in SENSORS],
    ).first().asDict()
    metrics = dict(counts)
    expected = None
    if counts:
        # rows that are neither corrupt nor key-less and not in clean were duplicates
        metrics["DuplicatesRemoved"] = (counts["RawRows"] - counts["CorruptRows"]
                                        - counts["MissingKeyRows"] - n_clean)
        expected = (counts["RawRows"] - counts["CorruptRows"] - counts["MissingKeyRows"]
                    - metrics["DuplicatesRemoved"])
    metrics.update({
        "CleanRows": n_clean,
        "InvalidValuesNulled": sum(stats[f"invalid_{s}"] for s in SENSORS),
        "ValuesImputed": sum(stats[f"imputed_{s}"] for s in SENSORS),
        "ValuesStillMissing": sum(stats[f"still_null_{s}"] for s in SENSORS),
        "AnomalyRows": anomalies.count(), "AlertEvents": alerts.count(),
        "HourlyRows": hourly.count(), "DailyRows": daily.count(),
    })

    checks = validate_outputs(clean_df, hourly, anomalies, daily, alerts=alerts,
                              expected_clean_rows=expected)
    metrics["ValidationFailures"] = sum(not c.passed for c in checks)
    timer.lap("validate")

    summary = {"run_id": run_id, "lake_root": lake_root, "skip_clean": skip_clean,
               "metrics": metrics, "detail": stats, "stage_seconds": timer.stages,
               "validation": [c.__dict__ for c in checks]}
    spark.createDataFrame([(run_id, json.dumps(summary))], "run_id string, summary string") \
        .coalesce(1).write.mode("append").json(p.run_metrics)
    if publish:
        # Only a few key metrics go to CloudWatch: custom metrics cost about
        # $0.30/metric/month beyond the 10 included in the free tier.
        publish_metrics({k: metrics[k] for k in CLOUDWATCH_METRICS if k in metrics})
    log.info("=== run %s metrics: %s", run_id, metrics)
    raise_on_failure(checks)
    return summary


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake-root", default=default_lake_root())
    ap.add_argument("--skip-clean", action="store_true",
                    help="reuse the existing clean layer and rebuild curated only")
    args = ap.parse_args(argv)
    from .spark_session import get_spark
    spark = get_spark()
    try:
        run(spark, args.lake_root, skip_clean=args.skip_clean)
        return 0
    except Exception:
        log.exception("Pipeline failed")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
