"""Publish pipeline metrics to Amazon CloudWatch.

On AWS Glue, everything the job logs already lands in CloudWatch Logs
(/aws-glue/jobs/output and /aws-glue/jobs/error). On top of that we publish
CUSTOM METRICS (numbers you can graph and alarm on), e.g. how many rows were
quarantined. A CloudWatch alarm on `ValidationFailures > 0` or on the Glue
job failing then notifies the team (see infra/cloudformation.yaml).

Locally (no AWS credentials) publishing is disabled and metrics are only
logged; set SENSOR_LAKE_CLOUDWATCH=1 to enable.
"""
from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)
NAMESPACE = "SensorLake"


def publish_metrics(metrics: dict[str, float], job_name: str = "sensor-lake-pipeline",
                    client=None) -> bool:
    if client is None and os.getenv("SENSOR_LAKE_CLOUDWATCH") != "1":
        log.info("CloudWatch publishing disabled; metrics: %s", metrics)
        return False
    if client is None:
        import boto3
        client = boto3.client("cloudwatch")
    data = [{"MetricName": name, "Value": float(value), "Unit": "Count",
             "Dimensions": [{"Name": "JobName", "Value": job_name}]}
            for name, value in metrics.items()]
    for i in range(0, len(data), 20):        # PutMetricData limit: 20 per call (safe batch)
        client.put_metric_data(Namespace=NAMESPACE, MetricData=data[i:i + 20])
    log.info("Published %d metrics to CloudWatch namespace %s", len(data), NAMESPACE)
    return True
