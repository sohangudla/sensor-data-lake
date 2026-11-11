-- The 10 longest alert events: the ones maintenance should look at first.
SELECT alert_id, machine_id, machine_type, sensor, direction,
       alert_start, alert_end, duration_min, anomaly_count,
       ROUND(peak_abs_z, 1) AS peak_abs_z
FROM sensor_lake.alerts
ORDER BY duration_min DESC, peak_abs_z DESC
LIMIT 10;
