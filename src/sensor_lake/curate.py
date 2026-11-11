"""CLEAN -> CURATED: anomaly detection, hourly metrics, daily performance."""
from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .config import (ALERT_GAP_S, ALERT_MIN_ANOMALIES, ALERT_WINDOW_S, ANOMALY_MIN_POINTS,
                     ANOMALY_WINDOW_S, ANOMALY_Z, EXPECTED_PER_HOUR,
                     HEALTH_CRITICAL_ALERT_MIN, HEALTH_MIN_UPTIME_PCT,
                     OPERATING_LIMITS, SENSORS,
                     SHIFT_END_HOUR, SHIFT_START_HOUR)


# ---------------------------------------------------------------- anomalies
BUCKET_S = 300   # 5-minute buckets for the rolling baseline


def add_rolling_zscores_exact(clean: DataFrame, window_s: int = ANOMALY_WINDOW_S) -> DataFrame:
    """REFERENCE implementation (v1, kept for tests and comparison).

    Sliding RANGE window over the previous `window_s` seconds per machine.
    Correct but slow: Spark recomputes avg/stddev over the ~720 rows in the
    frame for EVERY reading -> O(rows x window). 469 s on 6M readings locally.
    """
    w = (Window.partitionBy("machine_id").orderBy("epoch_s")
         .rangeBetween(-window_s, -1))
    cols = []
    for s in SENSORS:
        cols += [F.avg(s).over(w).alias(f"{s}_mean"),
                 F.stddev_samp(s).over(w).alias(f"{s}_std"),
                 F.count(s).over(w).alias(f"{s}_n")]
    return clean.select("*", *cols)


def add_rolling_zscores(clean: DataFrame, window_s: int = ANOMALY_WINDOW_S,
                        bucket_s: int = BUCKET_S) -> DataFrame:
    """PRODUCTION implementation (v2): pre-aggregated rolling baseline.

      1. group readings into 5-minute buckets per machine and keep only
         n, sum(x), sum(x^2) per sensor           (6M rows -> ~100k rows)
      2. roll those sums over the previous window_s/bucket_s buckets
         (the current bucket is excluded, so a fault cannot hide inside its
         own baseline)
      3. mean = S1/n ,  std = sqrt((S2 - S1^2/n) / (n-1))
      4. broadcast-join the small baseline table back to the readings
    Cost is O(rows), not O(rows x window).

    v3: the baseline is OPERATING-STATE aware. Buckets are partitioned by
    (machine, on_shift) and the window takes the previous N buckets *of the
    same state*. Before this, the +4 degC load step at every shift change was
    flagged for ~1 hour on every machine (71% of all false positives).
    """
    n_buckets = window_s // bucket_s
    hour = F.hour(F.col("event_ts"))
    readings = (clean.withColumn("_bucket", F.floor(F.col("epoch_s") / bucket_s))
                .withColumn("on_shift", (hour >= SHIFT_START_HOUR) & (hour < SHIFT_END_HOUR)))
    aggs = []
    for s in SENSORS:
        usable = F.when(~F.col(f"{s}_imputed"), F.col(s))   # imputed values are not evidence
        aggs += [F.count(usable).alias(f"{s}_c"), F.sum(usable).alias(f"{s}_s1"),
                 F.sum(usable * usable).alias(f"{s}_s2")]
    buckets = readings.groupBy("machine_id", "on_shift", "_bucket").agg(*aggs)

    # RANGE over bucket numbers: only same-state buckets from the last hour of
    # REAL time. (v3a used ROWS, which at 06:00 reached back to the previous
    # evening, a different phase of the daily temperature cycle, and still
    # raised 700+ false temperature alerts.) Right after a state change there
    # is no same-state history yet, n < ANOMALY_MIN_POINTS, and detection
    # pauses ~10 min: a deliberate warm-up period.
    w = (Window.partitionBy("machine_id", "on_shift").orderBy("_bucket")
         .rangeBetween(-n_buckets, -1))
    cols = []
    for s in SENSORS:
        n, s1, s2 = (F.sum(f"{s}_c").over(w), F.sum(f"{s}_s1").over(w),
                     F.sum(f"{s}_s2").over(w))
        var = (s2 - s1 * s1 / n) / (n - 1)
        cols += [n.alias(f"{s}_n"), (s1 / n).alias(f"{s}_mean"),
                 F.sqrt(F.greatest(var, F.lit(0.0))).alias(f"{s}_std")]
    baseline = buckets.select("machine_id", "on_shift", "_bucket", *cols)
    return (readings.join(F.broadcast(baseline), ["machine_id", "on_shift", "_bucket"], "left")
            .drop("_bucket"))


def detect_anomalies(clean: DataFrame, z_threshold: float = ANOMALY_Z,
                     min_points: int = ANOMALY_MIN_POINTS,
                     window_s: int = ANOMALY_WINDOW_S, exact: bool = False) -> DataFrame:
    """Long-format table: one row per (reading, sensor) whose |z| > threshold."""
    scored = (add_rolling_zscores_exact(clean, window_s) if exact
              else add_rolling_zscores(clean, window_s))
    per_sensor = []
    for s in SENSORS:
        z = (F.col(s) - F.col(f"{s}_mean")) / F.col(f"{s}_std")
        per_sensor.append(
            scored
            .filter(F.col(s).isNotNull() & ~F.col(f"{s}_imputed")
                    & (F.col(f"{s}_n") >= min_points) & (F.col(f"{s}_std") > 0))
            .withColumn("z_score", z)
            .filter(F.abs(F.col("z_score")) > z_threshold)
            .select("machine_id", "machine_type", "event_ts", "epoch_s", "event_date",
                    "event_hour", F.lit(s).alias("sensor"), F.col(s).alias("value"),
                    F.round(F.col(f"{s}_mean"), 4).alias("rolling_mean"),
                    F.round(F.col(f"{s}_std"), 4).alias("rolling_std"),
                    # full precision: rounding 3.004 to 3.00 broke the
                    # "|z| > 3" validation rule in the first run
                    F.col("z_score")))
    anomalies = reduce(DataFrame.unionByName, per_sensor)
    return anomalies.withColumn(
        "direction", F.when(F.col("z_score") > 0, "HIGH").otherwise("LOW")
    ).withColumn(
        "severity", F.when(F.abs("z_score") > 2 * z_threshold, "SEVERE").otherwise("MODERATE"))


# ---------------------------------------------------------------- alerts
def build_alerts(anomalies: DataFrame, min_anomalies: int = ALERT_MIN_ANOMALIES,
                 window_s: int = ALERT_WINDOW_S, gap_s: int = ALERT_GAP_S) -> DataFrame:
    """Turn point anomalies into ALERT EVENTS (what an operator would be paged for).

    1. sessionize: anomalies of the same machine+sensor less than gap_s apart
       form one candidate event (LAG + running SUM = "gaps and islands").
    2. persistence: keep only events that contain a burst of >= min_anomalies
       anomalies within window_s seconds. Random 3-sigma noise almost never
       repeats like that; real faults do.
    (v1 filtered rows BEFORE sessionizing, which produced events holding only
    1-2 anomalies: caught by the alerts_meet_persistence_rule validation.)
    """
    key = Window.partitionBy("machine_id", "sensor").orderBy("epoch_s")
    sessions = (anomalies
                .withColumn("_recent", F.count("*").over(key.rangeBetween(-window_s, 0)))
                .withColumn("_gap", F.col("epoch_s") - F.lag("epoch_s").over(key))
                .withColumn("_new", F.when(F.col("_gap").isNull() | (F.col("_gap") > gap_s), 1)
                            .otherwise(0))
                .withColumn("alert_seq", F.sum("_new").over(
                    key.rowsBetween(Window.unboundedPreceding, 0))))
    return (sessions.groupBy("machine_id", "machine_type", "sensor", "alert_seq")
            .agg(F.min("event_ts").alias("alert_start"), F.max("event_ts").alias("alert_end"),
                 F.count("*").alias("anomaly_count"),
                 F.max(F.abs("z_score")).alias("peak_abs_z"),
                 F.max("_recent").alias("_max_burst"),
                 # direction at the PEAK deviation. (first() was arbitrary, and
                 # the tail of an episode is often LOW: when a fault clears,
                 # the baseline still holds the high fault values.)
                 F.max_by("direction", F.abs("z_score")).alias("direction"),
                 F.min("epoch_s").alias("start_epoch"), F.max("epoch_s").alias("end_epoch"))
            .filter(F.col("_max_burst") >= min_anomalies).drop("_max_burst")
            .withColumn("duration_min", F.round((F.col("end_epoch") - F.col("start_epoch")) / 60, 1))
            .withColumn("alert_id", F.concat_ws("-", "machine_id", "sensor", "start_epoch"))
            .withColumn("event_date", F.to_date("alert_start"))
            .drop("alert_seq"))


# ---------------------------------------------------------------- hourly
def hourly_metrics(clean: DataFrame, anomalies: DataFrame) -> DataFrame:
    aggs = [F.count("*").alias("reading_count")]
    for s in SENSORS:
        aggs += [F.round(F.avg(s), 3).alias(f"avg_{s}"),
                 F.round(F.min(s), 3).alias(f"min_{s}"),
                 F.round(F.max(s), 3).alias(f"max_{s}"),
                 F.round(F.stddev_samp(s), 4).alias(f"std_{s}"),
                 F.sum(F.col(f"{s}_invalid").cast("int")).alias(f"invalid_{s}"),
                 F.sum(F.col(f"{s}_imputed").cast("int")).alias(f"imputed_{s}"),
                 F.sum(F.col(f"{s}_out_of_spec").cast("int")).alias(f"out_of_spec_{s}")]
    hourly = clean.groupBy("machine_id", "machine_type", "event_date", "event_hour").agg(*aggs)

    anom = (anomalies.groupBy("machine_id", "event_date", "event_hour")
            .agg(F.count("*").alias("anomaly_count")))
    return (hourly.join(anom, ["machine_id", "event_date", "event_hour"], "left")
            .fillna({"anomaly_count": 0})
            .withColumn("hour_start", F.to_timestamp(F.format_string(
                "%s %02d:00:00", F.col("event_date").cast("string"), F.col("event_hour"))))
            .withColumn("completeness_pct",
                        F.round(100.0 * F.col("reading_count") / EXPECTED_PER_HOUR, 2)))


# ---------------------------------------------------------------- daily
def daily_performance(hourly: DataFrame, alerts: DataFrame) -> DataFrame:
    """Daily machine health from the hourly table (aggregating an aggregate:
    cheap, because hourly is ~700x smaller than the readings) plus alerts.

    Health is driven by ALERTS and operating-limit breaches, not by raw point
    anomalies: 3-sigma on noisy 5-second data flags ~0.3% of normal readings
    (~140 per machine per day), so a point-anomaly count is mostly noise."""
    out_of_spec = sum(F.col(f"out_of_spec_{s}") for s in SENSORS)
    invalid = sum(F.col(f"invalid_{s}") for s in SENSORS)
    daily = (hourly
             .withColumn("_oos", out_of_spec).withColumn("_inv", invalid)
             .groupBy("machine_id", "machine_type", "event_date")
             .agg(F.sum("reading_count").alias("reading_count"),
                  F.countDistinct("event_hour").alias("hours_reporting"),
                  # weighted average: hours with more readings count more
                  F.round(F.sum(F.col("avg_temperature_c") * F.col("reading_count"))
                          / F.sum("reading_count"), 2).alias("avg_temperature_c"),
                  F.max("max_temperature_c").alias("max_temperature_c"),
                  F.round(F.sum(F.col("avg_vibration_mm_s") * F.col("reading_count"))
                          / F.sum("reading_count"), 3).alias("avg_vibration_mm_s"),
                  F.max("max_vibration_mm_s").alias("max_vibration_mm_s"),
                  F.min("min_pressure_bar").alias("min_pressure_bar"),
                  F.sum("anomaly_count").alias("anomaly_count"),
                  F.sum("_oos").alias("out_of_spec_readings"),
                  F.sum("_inv").alias("invalid_readings")))
    alert_counts = (alerts.groupBy("machine_id", "event_date")
                    .agg(F.count("*").alias("alert_count"),
                         F.round(F.sum("duration_min"), 1).alias("alert_minutes")))
    daily = (daily.join(alert_counts, ["machine_id", "event_date"], "left")
             .fillna({"alert_count": 0, "alert_minutes": 0.0}))
    uptime = F.round(100.0 * F.col("reading_count") / (EXPECTED_PER_HOUR * 24), 2)
    t_hi, v_hi = OPERATING_LIMITS["temperature_c"][1], OPERATING_LIMITS["vibration_mm_s"][1]
    p_lo = OPERATING_LIMITS["pressure_bar"][0]
    breach = ((F.col("max_temperature_c") > t_hi) | (F.col("max_vibration_mm_s") > v_hi)
              | (F.col("min_pressure_bar") < p_lo))
    return (daily.withColumn("uptime_pct", uptime)
            .withColumn("limit_breach", breach)
            .withColumn("health_status",
                        F.when(breach | (F.col("alert_minutes") >= HEALTH_CRITICAL_ALERT_MIN),
                               "CRITICAL")
                        .when((F.col("alert_count") >= 1)
                              | (F.col("uptime_pct") < HEALTH_MIN_UPTIME_PCT), "WARNING")
                        .otherwise("HEALTHY")))
