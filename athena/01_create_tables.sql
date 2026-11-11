-- Amazon Athena DDL for the curated layer (run once per environment).
-- Replace ${BUCKET} with the lake bucket. Alternatively a Glue Crawler can
-- create these tables automatically (see infra/cloudformation.yaml).
--
-- PARTITION PROJECTION: Athena computes the partition list from the rules
-- below instead of looking them up in the Glue catalog, so new daily
-- partitions are queryable immediately (no MSCK REPAIR / crawler run needed).

CREATE DATABASE IF NOT EXISTS sensor_lake;

CREATE EXTERNAL TABLE IF NOT EXISTS sensor_lake.hourly_machine_metrics (
  machine_id string, machine_type string, event_hour int, reading_count bigint,
  avg_temperature_c double, min_temperature_c double, max_temperature_c double, std_temperature_c double,
  invalid_temperature_c bigint, imputed_temperature_c bigint, out_of_spec_temperature_c bigint,
  avg_vibration_mm_s double, min_vibration_mm_s double, max_vibration_mm_s double, std_vibration_mm_s double,
  invalid_vibration_mm_s bigint, imputed_vibration_mm_s bigint, out_of_spec_vibration_mm_s bigint,
  avg_pressure_bar double, min_pressure_bar double, max_pressure_bar double, std_pressure_bar double,
  invalid_pressure_bar bigint, imputed_pressure_bar bigint, out_of_spec_pressure_bar bigint,
  anomaly_count bigint, hour_start timestamp, completeness_pct double)
PARTITIONED BY (event_date date)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/sensor-lake/curated/hourly_machine_metrics/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.event_date.type' = 'date',
  'projection.event_date.format' = 'yyyy-MM-dd',
  'projection.event_date.range' = '2026-01-01,NOW',
  'storage.location.template' = 's3://${BUCKET}/sensor-lake/curated/hourly_machine_metrics/event_date=${event_date}/');

CREATE EXTERNAL TABLE IF NOT EXISTS sensor_lake.daily_machine_performance (
  machine_id string, machine_type string, reading_count bigint, hours_reporting bigint,
  avg_temperature_c double, max_temperature_c double, avg_vibration_mm_s double,
  max_vibration_mm_s double, min_pressure_bar double, anomaly_count bigint,
  out_of_spec_readings bigint, invalid_readings bigint, alert_count bigint,
  alert_minutes double, uptime_pct double, limit_breach boolean, health_status string)
PARTITIONED BY (event_date date)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/sensor-lake/curated/daily_machine_performance/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.event_date.type' = 'date',
  'projection.event_date.format' = 'yyyy-MM-dd',
  'projection.event_date.range' = '2026-01-01,NOW',
  'storage.location.template' = 's3://${BUCKET}/sensor-lake/curated/daily_machine_performance/event_date=${event_date}/');

CREATE EXTERNAL TABLE IF NOT EXISTS sensor_lake.anomalies (
  machine_id string, machine_type string, event_ts timestamp, epoch_s bigint, event_hour int,
  sensor string, value double, rolling_mean double, rolling_std double, z_score double,
  direction string, severity string)
PARTITIONED BY (event_date date)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/sensor-lake/curated/anomalies/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.event_date.type' = 'date',
  'projection.event_date.format' = 'yyyy-MM-dd',
  'projection.event_date.range' = '2026-01-01,NOW',
  'storage.location.template' = 's3://${BUCKET}/sensor-lake/curated/anomalies/event_date=${event_date}/');

CREATE EXTERNAL TABLE IF NOT EXISTS sensor_lake.alerts (
  machine_id string, machine_type string, sensor string, alert_start timestamp,
  alert_end timestamp, anomaly_count bigint, peak_abs_z double, direction string,
  start_epoch bigint, end_epoch bigint, duration_min double, alert_id string)
PARTITIONED BY (event_date date)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/sensor-lake/curated/alerts/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.event_date.type' = 'date',
  'projection.event_date.format' = 'yyyy-MM-dd',
  'projection.event_date.range' = '2026-01-01,NOW',
  'storage.location.template' = 's3://${BUCKET}/sensor-lake/curated/alerts/event_date=${event_date}/');

CREATE EXTERNAL TABLE IF NOT EXISTS sensor_lake.dim_machine (
  machine_id string, machine_type string, plant string, production_line string, install_year bigint)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/sensor-lake/curated/dim_machine/';
