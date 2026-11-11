-- Machines with the most alert events and anomalies over the period, with plant info.
-- Filtering on the partition column event_date lets Athena skip other days
-- entirely (partition pruning) -> less data scanned -> lower cost.
SELECT d.machine_id,
       m.machine_type,
       m.plant,
       SUM(d.alert_count)                    AS alert_events,
       ROUND(SUM(d.alert_minutes), 1)        AS alert_minutes,
       SUM(d.anomaly_count)                  AS point_anomalies,
       SUM(CASE WHEN d.health_status = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_days
FROM sensor_lake.daily_machine_performance d
JOIN sensor_lake.dim_machine m ON m.machine_id = d.machine_id
WHERE d.event_date BETWEEN DATE '2026-11-01' AND DATE '2026-11-07'
GROUP BY d.machine_id, m.machine_type, m.plant
HAVING SUM(d.alert_count) > 0
ORDER BY alert_events DESC, alert_minutes DESC
LIMIT 10;
