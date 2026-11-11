-- Hourly vibration for the machine with the longest vibration alert,
-- around that alert, with a 3-hour moving average (window function).
WITH target AS (
    SELECT machine_id, alert_start
    FROM sensor_lake.alerts
    WHERE sensor = 'vibration_mm_s'
    ORDER BY duration_min DESC
    LIMIT 1
)
SELECT h.machine_id,
       h.hour_start,
       h.avg_vibration_mm_s,
       h.max_vibration_mm_s,
       ROUND(AVG(h.avg_vibration_mm_s) OVER (PARTITION BY h.machine_id ORDER BY h.hour_start
             ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 3) AS moving_avg_3h,
       h.anomaly_count
FROM sensor_lake.hourly_machine_metrics h
JOIN target t ON t.machine_id = h.machine_id
WHERE h.hour_start BETWEEN t.alert_start - INTERVAL '6' HOUR AND t.alert_start + INTERVAL '6' HOUR
ORDER BY h.hour_start;
