"""Automated validation of the written outputs (run after every pipeline run).

These are assertions about the DATA, read back from the lake, not about the
code. If any check fails the job exits non-zero (and on AWS a CloudWatch
alarm fires), so bad data is caught before a dashboard shows it.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import ALERT_MIN_ANOMALIES, ANOMALY_Z, EXPECTED_PER_HOUR, PHYSICAL_RANGE, SENSORS

log = logging.getLogger(__name__)


@dataclass
class Check:
    name: str
    passed: bool
    detail: str


class ValidationError(RuntimeError):
    pass


def validate_outputs(clean: DataFrame, hourly: DataFrame, anomalies: DataFrame,
                     daily: DataFrame, alerts: DataFrame | None = None,
                     expected_clean_rows: int | None = None) -> list[Check]:
    checks: list[Check] = []

    def add(name: str, ok: bool, detail: str) -> None:
        checks.append(Check(name, bool(ok), detail))
        log.log(logging.INFO if ok else logging.ERROR, "VALIDATION %-34s %s  %s",
                name, "PASS" if ok else "FAIL", detail)

    n_clean = clean.count()
    n_keys = clean.select("machine_id", "event_ts").distinct().count()
    add("clean_no_duplicate_keys", n_clean == n_keys, f"rows={n_clean} distinct_keys={n_keys}")
    if expected_clean_rows is not None:
        add("clean_row_reconciliation", n_clean == expected_clean_rows,
            f"written={n_clean} expected={expected_clean_rows}")

    out_of_range = clean.filter(" OR ".join(
        f"({s} < {lo} OR {s} > {hi})" for s, (lo, hi) in PHYSICAL_RANGE.items())).count()
    add("clean_values_physically_valid", out_of_range == 0, f"violations={out_of_range}")
    null_keys = clean.filter("machine_id IS NULL OR event_ts IS NULL").count()
    add("clean_no_null_keys", null_keys == 0, f"null_keys={null_keys}")

    over = hourly.filter(F.col("reading_count") > EXPECTED_PER_HOUR).count()
    add("hourly_count_not_above_expected", over == 0, f"hours_over_{EXPECTED_PER_HOUR}={over}")
    hourly_total = hourly.agg(F.sum("reading_count")).first()[0]
    add("hourly_reconciles_with_clean", hourly_total == n_clean,
        f"hourly_sum={hourly_total} clean={n_clean}")

    weak = anomalies.filter(F.abs("z_score") <= ANOMALY_Z).count()
    add("anomalies_exceed_threshold", weak == 0, f"rows_with_abs_z<={ANOMALY_Z}: {weak}")
    unknown = anomalies.filter(~F.col("sensor").isin(SENSORS)).count()
    add("anomalies_known_sensors", unknown == 0, f"unknown_sensor_rows={unknown}")

    if alerts is not None:
        short = alerts.filter(F.col("anomaly_count") < ALERT_MIN_ANOMALIES).count()
        add("alerts_meet_persistence_rule", short == 0, f"alerts_below_min={short}")
        backwards = alerts.filter(F.col("alert_end") < F.col("alert_start")).count()
        add("alerts_end_after_start", backwards == 0, f"invalid={backwards}")

    bad_status = daily.filter(~F.col("health_status").isin("HEALTHY", "WARNING", "CRITICAL")).count()
    add("daily_valid_health_status", bad_status == 0, f"invalid={bad_status}")
    bad_uptime = daily.filter((F.col("uptime_pct") < 0) | (F.col("uptime_pct") > 100)).count()
    add("daily_uptime_in_0_100", bad_uptime == 0, f"invalid={bad_uptime}")
    return checks


def raise_on_failure(checks: list[Check]) -> None:
    failed = [c for c in checks if not c.passed]
    if failed:
        raise ValidationError("Validation failed: " + "; ".join(f"{c.name} ({c.detail})" for c in failed))
