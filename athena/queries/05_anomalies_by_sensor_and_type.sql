-- Point anomalies by machine type and sensor, normalised per machine-day,
-- and the share that are SEVERE (|z| > 6).
SELECT a.machine_type,
       a.sensor,
       COUNT(*)                                                        AS anomalies,
       ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT a.machine_id || CAST(a.event_date AS varchar)), 1)
                                                                       AS per_machine_day,
       ROUND(100.0 * SUM(CASE WHEN a.severity = 'SEVERE' THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                                       AS severe_pct
FROM sensor_lake.anomalies a
WHERE a.event_date BETWEEN DATE '2026-11-01' AND DATE '2026-11-07'
GROUP BY a.machine_type, a.sensor
ORDER BY a.machine_type, a.sensor;
