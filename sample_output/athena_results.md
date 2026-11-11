# Athena queries executed locally with DuckDB


## 01_machines_most_alerts

10 rows, 1367 ms

```
machine_id machine_type   plant  alert_events  alert_minutes  point_anomalies  critical_days
      M019     conveyor PLANT-A           6.0           41.6           1222.0            2.0
      M043     conveyor PLANT-B           4.0           39.3           1522.0            1.0
      M011     conveyor PLANT-A           4.0           31.8           1552.0            1.0
      M034   compressor PLANT-B           4.0           16.8           1213.0            1.0
      M006   compressor PLANT-A           4.0            7.7           1267.0            0.0
      M005         pump PLANT-A           3.0           35.7           1510.0            1.0
      M027     conveyor PLANT-B           3.0           33.0           1544.0            1.0
      M020     cnc_mill PLANT-A           3.0           10.5           1229.0            0.0
      M049         pump PLANT-B           3.0           10.2           1242.0            0.0
      M004     cnc_mill PLANT-A           3.0            2.2           1210.0            0.0
```

## 02_daily_uptime

7 rows, 35 ms

```
event_date  fleet_avg_uptime_pct worst_machine  worst_uptime_pct  machines_below_95pct
2026-11-01                 99.96          M001             98.19                   0.0
2026-11-02                 99.99          M048             99.98                   0.0
2026-11-03                 99.89          M049             95.95                   0.0
2026-11-04                 99.77          M025             94.02                   2.0
2026-11-05                 99.89          M031             94.79                   1.0
2026-11-06                 99.96          M013             98.18                   0.0
2026-11-07                 99.81          M043             95.13                   0.0
```

## 03_fleet_health_by_day

7 rows, 15 ms

```
event_date  healthy  warning  critical  machines
2026-11-01     39.0      8.0       3.0        50
2026-11-02     38.0     11.0       1.0        50
2026-11-03     40.0      8.0       2.0        50
2026-11-04     34.0     14.0       2.0        50
2026-11-05     40.0      7.0       3.0        50
2026-11-06     41.0      7.0       2.0        50
2026-11-07     38.0      7.0       5.0        50
```

## 04_vibration_trend_with_moving_avg

12 rows, 39 ms

```
machine_id          hour_start  avg_vibration_mm_s  max_vibration_mm_s  moving_avg_3h  anomaly_count
      M035 2026-11-06 17:00:00               1.546               1.904          1.546              9
      M035 2026-11-06 18:00:00               1.556               2.003          1.551              6
      M035 2026-11-06 19:00:00               1.553               1.966          1.552             16
      M035 2026-11-06 20:00:00               1.535               1.881          1.548             10
      M035 2026-11-06 21:00:00               1.547               1.910          1.545              8
      M035 2026-11-06 22:00:00               1.346               1.676          1.476              6
      M035 2026-11-06 23:00:00               7.687              12.016          3.527            290
      M035 2026-11-07 00:00:00               1.471              11.947          3.501              4
      M035 2026-11-07 01:00:00               1.343               1.775          3.500              4
      M035 2026-11-07 02:00:00               1.353               1.733          1.389              7
      M035 2026-11-07 03:00:00               1.345               1.755          1.347             11
      M035 2026-11-07 04:00:00               1.352               1.739          1.350             12
```

## 05_anomalies_by_sensor_and_type

12 rows, 52 ms

```
machine_type         sensor  anomalies  per_machine_day  severe_pct
    cnc_mill   pressure_bar       4008             47.7         3.5
    cnc_mill  temperature_c       6645             79.1         2.0
    cnc_mill vibration_mm_s       4019             47.8         3.5
  compressor   pressure_bar       4421             48.6         3.3
  compressor  temperature_c       7169             78.8         2.2
  compressor vibration_mm_s       4241             46.6         3.4
    conveyor   pressure_bar       4205             50.1         6.5
    conveyor  temperature_c       6492             77.3         2.4
    conveyor vibration_mm_s       5619             66.9         9.5
        pump   pressure_bar       4356             47.9         3.6
        pump  temperature_c       8232             90.5         4.5
        pump vibration_mm_s       4336             47.6         3.3
```

## 06_longest_alerts

10 rows, 20 ms

```
                      alert_id machine_id machine_type         sensor direction         alert_start           alert_end  duration_min  anomaly_count  peak_abs_z
 M029-temperature_c-1793891185       M029         pump  temperature_c      HIGH 2026-11-05 15:06:25 2026-11-05 15:39:55          33.5            276         8.6
M035-vibration_mm_s-1794005945       M035     conveyor vibration_mm_s      HIGH 2026-11-06 22:59:05 2026-11-06 23:29:55          30.8            288        11.6
M027-vibration_mm_s-1793798065       M027     conveyor vibration_mm_s      HIGH 2026-11-04 13:14:25 2026-11-04 13:44:55          30.5            287        13.2
M011-vibration_mm_s-1793933745       M011     conveyor vibration_mm_s      HIGH 2026-11-06 02:55:45 2026-11-06 03:24:55          29.2            290        10.4
M043-vibration_mm_s-1793873150       M043     conveyor vibration_mm_s      HIGH 2026-11-05 10:05:50 2026-11-05 10:34:55          29.1            290        10.2
 M005-temperature_c-1793814100       M005         pump  temperature_c      HIGH 2026-11-04 17:41:40 2026-11-04 18:09:55          28.3            283        11.4
M003-vibration_mm_s-1793736715       M003     conveyor vibration_mm_s      HIGH 2026-11-03 20:11:55 2026-11-03 20:39:55          28.0            292        12.2
 M017-temperature_c-1794091665       M017         pump  temperature_c      HIGH 2026-11-07 22:47:45 2026-11-07 23:14:55          27.2            271         9.0
 M041-temperature_c-1794029620       M041         pump  temperature_c      HIGH 2026-11-07 05:33:40 2026-11-07 05:59:55          26.3            275         9.8
 M019-temperature_c-1793732700       M019     conveyor  temperature_c       LOW 2026-11-03 19:05:00 2026-11-03 19:19:05          14.1              7         3.7
```