import gzip
import json

import boto3
import pandas as pd
from moto import mock_aws

from sensor_lake.generator import GeneratorConfig, generate
from sensor_lake.monitoring import NAMESPACE, publish_metrics


def test_generator_injects_expected_problems(tmp_path):
    cfg = GeneratorConfig(machines=4, days=1, seed=3)
    stats = generate(tmp_path / "lake", cfg, tmp_path / "truth")
    assert stats["unique_readings"] <= 4 * 86400 // 5
    assert stats["lines_written"] == stats["unique_readings"] + stats["duplicates_injected"]
    files = sorted((tmp_path / "lake" / "raw" / "sensor_readings").rglob("*.json.gz"))
    assert len(files) >= 24
    first = json.loads(gzip.open(files[0], "rt").readline())
    assert {"machine_id", "event_ts", "temperature_c", "vibration_mm_s", "pressure_bar"} <= set(first)
    truth = pd.read_csv(tmp_path / "truth" / "fault_episodes.csv")
    assert (truth["end_epoch"] >= truth["start_epoch"]).all()


def test_generator_is_deterministic(tmp_path):
    a = generate(tmp_path / "a", GeneratorConfig(machines=2, days=1, seed=9), tmp_path / "ta")
    b = generate(tmp_path / "b", GeneratorConfig(machines=2, days=1, seed=9), tmp_path / "tb")
    assert a == b


@mock_aws
def test_publish_metrics_to_cloudwatch():
    client = boto3.client("cloudwatch", region_name="us-east-1")
    metrics = {f"Metric{i}": i for i in range(25)}            # > 20 -> two API calls
    assert publish_metrics(metrics, client=client)
    names = {m["MetricName"] for m in client.list_metrics(Namespace=NAMESPACE)["Metrics"]}
    assert names == set(metrics)


def test_publish_disabled_without_flag(monkeypatch):
    monkeypatch.delenv("SENSOR_LAKE_CLOUDWATCH", raising=False)
    assert publish_metrics({"x": 1}) is False
