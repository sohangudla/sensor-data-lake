from datetime import datetime, timezone

import pytest


@pytest.fixture(scope="session")
def spark():
    from sensor_lake.spark_session import get_spark
    s = get_spark("sensor-lake-tests", master="local[2]", shuffle_partitions=2,
                  driver_memory="2g")
    yield s
    s.stop()


T0 = int(datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc).timestamp())   # on-shift hour


def clean_rows(spark, values, machine="M001", start=T0, step=5, sensor="temperature_c"):
    """Build a clean-layer-shaped DataFrame for one machine from a list of
    values of one sensor (others held constant)."""
    from pyspark.sql import functions as F
    rows = []
    for i, v in enumerate(values):
        rec = {"temperature_c": 60.0, "vibration_mm_s": 2.0, "pressure_bar": 6.0}
        rec[sensor] = v
        rows.append((machine, "pump", start + i * step, rec["temperature_c"],
                     rec["vibration_mm_s"], rec["pressure_bar"]))
    df = spark.createDataFrame(rows, "machine_id string, machine_type string, epoch_s long, "
                                     "temperature_c double, vibration_mm_s double, pressure_bar double")
    df = df.withColumn("event_ts", F.col("epoch_s").cast("timestamp"))
    for s in ("temperature_c", "vibration_mm_s", "pressure_bar"):
        for f in ("invalid", "out_of_spec", "imputed"):
            df = df.withColumn(f"{s}_{f}", F.lit(False))
    return (df.withColumn("event_date", F.to_date("event_ts"))
              .withColumn("event_hour", F.hour("event_ts")))
