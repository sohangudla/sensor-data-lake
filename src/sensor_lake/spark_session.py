"""SparkSession factory used by the jobs and the tests."""
from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from .config import PROJECT_ROOT

SHIM_JAR = PROJECT_ROOT / "windows_compat" / "windows-localfs.jar"


def get_spark(app_name: str = "sensor-lake", master: str = "local[*]",
              shuffle_partitions: int = 16, driver_memory: str = "4g") -> SparkSession:
    # make the executors use the same Python as the driver
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.appName(app_name).master(master)
        .config("spark.driver.memory", driver_memory)
        # default 200 shuffle partitions is tuned for clusters; far too many locally
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        # rewriting a partitioned table only replaces the partitions present in
        # the new data -> reruns are idempotent and don't wipe other days
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if os.name == "nt":
        builder = _windows_local_fs(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _windows_local_fs(builder: SparkSession.Builder) -> SparkSession.Builder:
    """See windows_compat/src/WindowsLocalFileSystem.java for why this exists."""
    if not SHIM_JAR.exists():
        sys.path.insert(0, str(PROJECT_ROOT / "windows_compat"))
        from build import build  # type: ignore
        build()
    return (builder
            .config("spark.driver.extraClassPath", str(SHIM_JAR))
            .config("spark.hadoop.fs.file.impl", "dev.sensorlake.hadoop.WindowsLocalFileSystem")
            .config("spark.hadoop.fs.file.impl.disable.cache", "true"))
