"""RAW -> CLEAN transformations (PySpark).

Each function takes and returns a DataFrame so it can be unit-tested on a
handful of rows. Nothing here triggers a Spark job by itself: these are
TRANSFORMATIONS (lazy). Jobs only run when the pipeline calls an ACTION
such as .write or .count().
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import FFILL_MAX_AGE_S, OPERATING_LIMITS, PHYSICAL_RANGE, SENSORS
from .schemas import CORRUPT_COL, RAW_READING_SCHEMA

TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"


def read_raw_readings(spark: SparkSession, path: str) -> DataFrame:
    """Read gzipped JSON lines with an explicit schema. Malformed lines are
    kept (PERMISSIVE mode) in _corrupt_record instead of failing the job.
    The ingest_date=... folder becomes a column via partition discovery."""
    return (spark.read
            .schema(RAW_READING_SCHEMA)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_COL)
            .json(path)
            .withColumn("source_file", F.input_file_name()))


def split_corrupt(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    corrupt = df.filter(F.col(CORRUPT_COL).isNotNull())
    good = df.filter(F.col(CORRUPT_COL).isNull()).drop(CORRUPT_COL)
    return good, corrupt


def parse_keys(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Parse the event timestamp; rows without a usable key (machine_id +
    timestamp) cannot be placed in time and go to quarantine."""
    parsed = (df.withColumn("event_ts", F.to_timestamp("event_ts", TS_FORMAT))
                .withColumn("epoch_s", F.col("event_ts").cast("long")))
    bad = F.col("machine_id").isNull() | F.col("event_ts").isNull()
    return parsed.filter(~bad), parsed.filter(bad)


def deduplicate(df: DataFrame) -> DataFrame:
    """One row per (machine_id, event_ts). If copies differ, the most recently
    ingested one wins. ROW_NUMBER is used instead of dropDuplicates so that
    choice is explicit and deterministic."""
    w = Window.partitionBy("machine_id", "event_ts").orderBy(
        F.col("ingest_date").desc(), F.col("source_file").desc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter("_rn = 1").drop("_rn"))


def validate_ranges(df: DataFrame) -> DataFrame:
    """Two levels of validation per sensor:
      <s>_invalid     value outside the PHYSICAL range -> sensor fault; value set NULL
      <s>_out_of_spec value possible but outside OPERATING limits -> machine alarm
    """
    for s in SENSORS:
        lo, hi = PHYSICAL_RANGE[s]
        invalid = F.col(s).isNotNull() & ((F.col(s) < lo) | (F.col(s) > hi))
        df = (df.withColumn(f"{s}_invalid", invalid)
                .withColumn(s, F.when(invalid, F.lit(None)).otherwise(F.col(s))))
        olo, ohi = OPERATING_LIMITS[s]
        df = df.withColumn(f"{s}_out_of_spec",
                           F.coalesce((F.col(s) < olo) | (F.col(s) > ohi), F.lit(False)))
    return df


def fill_short_gaps(df: DataFrame, max_age_s: int = FFILL_MAX_AGE_S) -> DataFrame:
    """Forward-fill a missing value with the machine's last good reading, but
    only if that reading is at most `max_age_s` seconds old (a RANGE window on
    epoch seconds, so real time is used, not row counts). Longer gaps stay NULL
    because inventing minutes of sensor data would hide real outages."""
    w = (Window.partitionBy("machine_id").orderBy("epoch_s")
         .rangeBetween(-max_age_s, -1))
    for s in SENSORS:
        last_good = F.last(F.col(s), ignorenulls=True).over(w)
        df = (df.withColumn(f"{s}_imputed", F.col(s).isNull() & last_good.isNotNull())
                .withColumn(s, F.coalesce(F.col(s), last_good)))
    return df


def add_partitions(df: DataFrame) -> DataFrame:
    return (df.withColumn("event_date", F.to_date("event_ts"))
              .withColumn("event_hour", F.hour("event_ts")))


CLEAN_COLUMNS = (["reading_id", "machine_id", "machine_type", "plant", "event_ts", "epoch_s",
                  "event_hour"] + SENSORS
                 + [f"{s}_{f}" for s in SENSORS for f in ("invalid", "out_of_spec", "imputed")]
                 + ["firmware", "ingest_date", "event_date"])


def clean_readings(df_good: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Full clean chain on parsed, non-corrupt rows. Returns (clean, bad_keys)."""
    keyed, bad_keys = parse_keys(df_good)
    out = add_partitions(fill_short_gaps(validate_ranges(deduplicate(keyed))))
    return out.select(*CLEAN_COLUMNS), bad_keys
