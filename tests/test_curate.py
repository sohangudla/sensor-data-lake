import numpy as np
import pytest
from pyspark.sql import functions as F

from sensor_lake import curate as K
from tests.conftest import T0, clean_rows


def noise(n, seed=1, mean=60.0, sd=0.5):
    # plain Python floats: Spark's DoubleType rejects numpy.float64
    return [float(x) for x in np.random.default_rng(seed).normal(mean, sd, n).round(3)]


def test_spike_is_detected_after_warm_up(spark):
    vals = noise(1000)            # ~83 minutes of 5-second data
    vals[900] = 75.0              # +30 sigma spike
    anoms = K.detect_anomalies(clean_rows(spark, vals)).collect()
    hits = [a for a in anoms if a["epoch_s"] == T0 + 900 * 5]
    assert len(hits) == 1 and hits[0]["direction"] == "HIGH" and hits[0]["z_score"] > 20
    assert hits[0]["severity"] == "SEVERE"


def test_no_detection_during_warm_up(spark):
    vals = noise(100)             # < 10 minutes of history: baseline not trusted
    vals[90] = 90.0
    assert K.detect_anomalies(clean_rows(spark, vals)).count() == 0


def test_bucketed_baseline_matches_definition(spark):
    """The mean/std used for a reading must equal the plain mean/std of the
    previous 12 five-minute buckets (computed here with numpy)."""
    vals = noise(1000, seed=7)
    scored = K.add_rolling_zscores(clean_rows(spark, vals))
    i = 950
    row = scored.filter(F.col("epoch_s") == T0 + i * 5).first()
    bucket = (T0 + i * 5) // 300
    prev = [v for k, v in enumerate(vals) if bucket - 12 <= (T0 + k * 5) // 300 < bucket]
    assert row["temperature_c_n"] == len(prev)
    assert row["temperature_c_mean"] == pytest.approx(np.mean(prev), abs=1e-9)
    assert row["temperature_c_std"] == pytest.approx(np.std(prev, ddof=1), rel=1e-6)


def anomaly_frame(spark, epochs, sensor="vibration_mm_s"):
    rows = [("M001", "pump", T0 + e, sensor, 9.9, 4.0) for e in epochs]
    return (spark.createDataFrame(rows, "machine_id string, machine_type string, epoch_s long, "
                                        "sensor string, value double, z_score double")
            .withColumn("event_ts", F.col("epoch_s").cast("timestamp"))
            .withColumn("direction", F.lit("HIGH")))


def test_isolated_anomalies_do_not_alert(spark):
    assert K.build_alerts(anomaly_frame(spark, [0, 400, 900, 2000])).count() == 0


def test_persistent_burst_is_one_alert_and_separate_bursts_are_two(spark):
    one = K.build_alerts(anomaly_frame(spark, [0, 5, 10, 15, 20])).collect()
    assert len(one) == 1 and one[0]["anomaly_count"] == 5 and one[0]["duration_min"] == pytest.approx(0.3)
    two = K.build_alerts(anomaly_frame(spark, [0, 5, 10, 1000, 1005, 1010]))
    assert two.count() == 2


def test_hourly_metrics_and_completeness(spark):
    clean = clean_rows(spark, [60.0] * 360 + [70.0] * 360)   # exactly one hour
    anoms = anomaly_frame(spark, [10]).withColumn("event_date", F.to_date("event_ts")) \
                                      .withColumn("event_hour", F.hour("event_ts"))
    h = K.hourly_metrics(clean, anoms).collect()
    assert len(h) == 1
    h = h[0]
    assert h["reading_count"] == 720 and h["completeness_pct"] == 100.0
    assert h["avg_temperature_c"] == 65.0 and h["max_temperature_c"] == 70.0
    assert h["anomaly_count"] == 1


def test_daily_health_rules(spark):
    clean = clean_rows(spark, [60.0] * 720)
    no_anoms = anomaly_frame(spark, []).withColumn("event_date", F.to_date("event_ts")) \
                                       .withColumn("event_hour", F.hour("event_ts"))
    hourly = K.hourly_metrics(clean, no_anoms)
    alerts = K.build_alerts(anomaly_frame(spark, []))
    d = K.daily_performance(hourly, alerts).first()
    # only 1 hour of data in the day -> uptime ~4% -> WARNING
    assert d["uptime_pct"] == pytest.approx(100 / 24, abs=0.01)
    assert d["health_status"] == "WARNING" and d["alert_count"] == 0

    hot = clean_rows(spark, [60.0] * 700 + [95.0] * 20)   # above the 90 C operating limit
    d2 = K.daily_performance(K.hourly_metrics(hot, no_anoms), alerts).first()
    assert d2["limit_breach"] and d2["health_status"] == "CRITICAL"
