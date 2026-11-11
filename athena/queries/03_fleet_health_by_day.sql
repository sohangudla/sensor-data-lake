-- Fleet health status per day (pivot with conditional aggregation).
SELECT event_date,
       SUM(CASE WHEN health_status = 'HEALTHY'  THEN 1 ELSE 0 END) AS healthy,
       SUM(CASE WHEN health_status = 'WARNING'  THEN 1 ELSE 0 END) AS warning,
       SUM(CASE WHEN health_status = 'CRITICAL' THEN 1 ELSE 0 END) AS critical,
       COUNT(*)                                                    AS machines
FROM sensor_lake.daily_machine_performance
WHERE event_date BETWEEN DATE '2026-11-01' AND DATE '2026-11-07'
GROUP BY event_date
ORDER BY event_date;
