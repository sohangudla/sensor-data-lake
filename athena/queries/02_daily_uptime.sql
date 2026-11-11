-- Uptime per day: fleet average, worst machine, and machines below 95 %.
-- Uptime here = readings received / readings expected (data availability).
WITH ranked AS (
    SELECT event_date, machine_id, uptime_pct,
           ROW_NUMBER() OVER (PARTITION BY event_date ORDER BY uptime_pct ASC) AS rn
    FROM sensor_lake.daily_machine_performance
    WHERE event_date BETWEEN DATE '2026-11-01' AND DATE '2026-11-07'
)
SELECT event_date,
       ROUND(AVG(uptime_pct), 2)                                   AS fleet_avg_uptime_pct,
       MAX(CASE WHEN rn = 1 THEN machine_id END)                   AS worst_machine,
       MIN(uptime_pct)                                             AS worst_uptime_pct,
       SUM(CASE WHEN uptime_pct < 95 THEN 1 ELSE 0 END)            AS machines_below_95pct
FROM ranked
GROUP BY event_date
ORDER BY event_date;
