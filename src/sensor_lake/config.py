"""Configuration for the sensor data lake.

The lake root can be a local folder (default) or an S3 URI such as
s3://my-bucket/sensor-lake ; every path below is derived from it, so the
same code runs locally and on AWS Glue.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class LakePaths:
    root: str

    def _join(self, *parts: str) -> str:
        return "/".join([self.root.rstrip("/"), *parts])

    @property
    def raw_readings(self) -> str: return self._join("raw", "sensor_readings")
    @property
    def raw_reference(self) -> str: return self._join("raw", "reference", "machines")
    @property
    def clean_readings(self) -> str: return self._join("clean", "sensor_readings")
    @property
    def quarantine(self) -> str: return self._join("clean", "_quarantine")
    @property
    def curated_alerts(self) -> str: return self._join("curated", "alerts")
    @property
    def curated_hourly(self) -> str: return self._join("curated", "hourly_machine_metrics")
    @property
    def curated_anomalies(self) -> str: return self._join("curated", "anomalies")
    @property
    def curated_daily(self) -> str: return self._join("curated", "daily_machine_performance")
    @property
    def curated_machines(self) -> str: return self._join("curated", "dim_machine")
    @property
    def run_metrics(self) -> str: return self._join("curated", "_pipeline_runs")


def default_lake_root() -> str:
    return os.getenv("LAKE_ROOT", (PROJECT_ROOT / "lake").as_posix())


# ------------------------------------------------------------------ sensors
SENSORS = ["temperature_c", "vibration_mm_s", "pressure_bar"]

# Physically possible range of each sensor. Outside this, the value cannot be
# real -> it is a SENSOR FAULT (stuck/disconnected/sentinel value) and is nulled.
PHYSICAL_RANGE = {
    "temperature_c": (-40.0, 150.0),
    "vibration_mm_s": (0.0, 50.0),
    "pressure_bar": (0.0, 20.0),
}

# Normal operating limits. Outside this, the value is real but the MACHINE is
# outside its spec -> an operational alarm (flagged, not removed).
OPERATING_LIMITS = {
    "temperature_c": (10.0, 90.0),
    "vibration_mm_s": (0.0, 7.1),
    "pressure_bar": (3.0, 10.0),
}

READING_INTERVAL_S = 5              # one reading per machine every 5 seconds
EXPECTED_PER_HOUR = 3600 // READING_INTERVAL_S

# Missing-value handling: fill a gap only if the last good value is at most
# this many seconds old (forward fill). Longer gaps stay NULL.
FFILL_MAX_AGE_S = 15

# Anomaly detection: rolling 3-sigma on the previous hour of readings.
ANOMALY_WINDOW_S = 3600
ANOMALY_Z = 3.0
ANOMALY_MIN_POINTS = 120            # don't trust a window with < 10 min of data

# Plant schedule (reference data): machines run under load 06:00-22:00 UTC.
# The rolling baseline is computed separately for on-shift and off-shift
# periods, otherwise every shift change looks like an anomaly.
SHIFT_START_HOUR = 6
SHIFT_END_HOUR = 22

# Alerts: an anomaly becomes an ALERT only if it persists, i.e. at least
# ALERT_MIN_ANOMALIES anomalous readings of the same machine+sensor within
# ALERT_WINDOW_S seconds. Anomalies further apart than ALERT_GAP_S start a
# new alert event.
ALERT_MIN_ANOMALIES = 3
ALERT_WINDOW_S = 60
ALERT_GAP_S = 300

# Daily health classification
#   CRITICAL: an operating limit was breached, or alerts lasted >= 10 min in total
#   WARNING : any alert, or uptime (data received / expected) below 95%
HEALTH_CRITICAL_ALERT_MIN = 10.0
HEALTH_MIN_UPTIME_PCT = 95.0
