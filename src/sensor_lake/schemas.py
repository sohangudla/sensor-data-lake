"""Explicit schemas. Never let Spark infer the schema of production JSON:
inference scans the data (slow) and silently changes types when a bad value
appears (e.g. a whole column becomes string)."""
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField, StructType)

CORRUPT_COL = "_corrupt_record"

RAW_READING_SCHEMA = StructType([
    StructField("reading_id", StringType()),
    StructField("machine_id", StringType()),
    StructField("machine_type", StringType()),
    StructField("plant", StringType()),
    StructField("event_ts", StringType()),        # parsed explicitly in the clean step
    StructField("temperature_c", DoubleType()),
    StructField("vibration_mm_s", DoubleType()),
    StructField("pressure_bar", DoubleType()),
    StructField("firmware", StringType()),
    StructField(CORRUPT_COL, StringType()),       # PERMISSIVE mode puts bad lines here
])

MACHINE_SCHEMA = StructType([
    StructField("machine_id", StringType()),
    StructField("machine_type", StringType()),
    StructField("plant", StringType()),
    StructField("production_line", StringType()),
    StructField("install_year", LongType()),
])
