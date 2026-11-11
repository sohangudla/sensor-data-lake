"""Machine health dashboard (Streamlit).

Reads the CURATED layer through DuckDB (the same SQL views the Athena
queries use). On AWS the same queries would run against Athena instead.

    streamlit run dashboard/app.py
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from sensor_lake.local_sql import connect

st.set_page_config(page_title="Machine Health", layout="wide")

# Validated reference palette (dataviz skill, light mode)
SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]
STATUS = {"HEALTHY": "#0ca30c", "WARNING": "#fab219", "CRITICAL": "#d03b3b"}
STATUS_ICON = {"HEALTHY": "✅ Healthy", "WARNING": "⚠️ Warning", "CRITICAL": "⛔ Critical"}
GRID, MUTED, INK = "#e1e0d9", "#898781", "#0b0b0b"


def style(fig: go.Figure, height: int = 320) -> go.Figure:
    fig.update_layout(height=height, margin=dict(l=8, r=8, t=40, b=8),
                      plot_bgcolor="#fcfcfb", paper_bgcolor="#fcfcfb",
                      font=dict(color=INK, size=13), hovermode="x unified",
                      legend=dict(orientation="h", y=-0.15, yanchor="top", x=0, title_text=""))
    fig.update_xaxes(showgrid=False, linecolor="#c3c2b7", tickfont=dict(color=MUTED))
    fig.update_yaxes(gridcolor=GRID, zeroline=False, tickfont=dict(color=MUTED))
    return fig


@st.cache_resource
def db():
    return connect()


@st.cache_data
def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    # parameterised (?) queries: user selections are never pasted into SQL text
    return db().execute(sql, list(params)).fetchdf()


daily = q("""SELECT d.*, m.plant, m.production_line FROM sensor_lake.daily_machine_performance d
             JOIN sensor_lake.dim_machine m USING (machine_id)""")
daily["event_date"] = pd.to_datetime(daily["event_date"]).dt.date

# ------------------------------------------------------------ filters (one row)
st.title("Manufacturing machine health")
st.caption("50 simulated machines · 5-second sensor readings · curated layer of the sensor data lake")
f1, f2, f3 = st.columns([2, 1, 1])
dates = sorted(daily["event_date"].unique())
start, end = f1.select_slider("Date range", options=dates, value=(dates[0], dates[-1]))
plant = f2.selectbox("Plant", ["All"] + sorted(daily["plant"].unique()))
machine = f3.selectbox("Machine (trends)", sorted(daily["machine_id"].unique()),
                       index=sorted(daily["machine_id"].unique()).index("M035"))

sel = daily[(daily.event_date >= start) & (daily.event_date <= end)]
if plant != "All":
    sel = sel[sel.plant == plant]
latest = sel[sel.event_date == sel.event_date.max()]

# ------------------------------------------------------------ KPI tiles
k = st.columns(5)
k[0].metric("Machines", sel.machine_id.nunique())
k[1].metric("Critical (latest day)", int((latest.health_status == "CRITICAL").sum()))
k[2].metric("Warning (latest day)", int((latest.health_status == "WARNING").sum()))
k[3].metric("Fleet data uptime", f"{sel.uptime_pct.mean():.2f}%")
k[4].metric("Alert events", int(sel.alert_count.sum()))

# ------------------------------------------------------------ machine health
st.subheader("Machine health")
c1, c2 = st.columns([3, 2])
grid = sel.pivot_table(index="machine_id", columns="event_date", values="health_status",
                       aggfunc="first")
code = {"HEALTHY": 0, "WARNING": 1, "CRITICAL": 2}
heat = go.Figure(go.Heatmap(
    z=grid.replace(code).astype(float).values, x=[str(c) for c in grid.columns], y=grid.index,
    zmin=0, zmax=2, xgap=2, ygap=2, showscale=False,
    colorscale=[[0, STATUS["HEALTHY"]], [0.33, STATUS["HEALTHY"]], [0.34, STATUS["WARNING"]],
                [0.66, STATUS["WARNING"]], [0.67, STATUS["CRITICAL"]], [1, STATUS["CRITICAL"]]],
    customdata=grid.values,
    hovertemplate="%{y} · %{x}<br>Status: %{customdata}<extra></extra>"))
heat.update_layout(title="Daily status per machine (green healthy · amber warning · red critical)",
                   hovermode="closest")
c1.plotly_chart(style(heat, 900), use_container_width=True)

table = (latest[["machine_id", "machine_type", "plant", "health_status", "uptime_pct",
                 "alert_count", "max_temperature_c", "max_vibration_mm_s", "min_pressure_bar"]]
         .assign(health_status=lambda d: d.health_status.map(STATUS_ICON))
         .sort_values(["alert_count", "max_vibration_mm_s"], ascending=False))
c2.markdown(f"**Latest day ({latest.event_date.max()})**, worst first")
c2.dataframe(table, hide_index=True, height=860, use_container_width=True)

# ------------------------------------------------------------ trends
st.subheader(f"Temperature and vibration trends: {machine}")
hourly = q("""SELECT hour_start, avg_temperature_c, max_temperature_c, avg_vibration_mm_s,
                      max_vibration_mm_s, anomaly_count
               FROM sensor_lake.hourly_machine_metrics
               WHERE machine_id = ? AND event_date BETWEEN ? AND ?
               ORDER BY hour_start""", (machine, start, end))
t1, t2 = st.columns(2)
for col, sensor, unit in ((t1, "temperature_c", "°C"), (t2, "vibration_mm_s", "mm/s")):
    fig = go.Figure()
    fig.add_scatter(x=hourly.hour_start, y=hourly[f"avg_{sensor}"], name="Hourly average",
                    line=dict(color=SERIES[0], width=2))
    fig.add_scatter(x=hourly.hour_start, y=hourly[f"max_{sensor}"], name="Hourly maximum",
                    line=dict(color=SERIES[1], width=2))
    fig.update_layout(title=f"{sensor.split('_')[0].capitalize()} ({unit})")
    col.plotly_chart(style(fig), use_container_width=True)

# ------------------------------------------------------------ abnormal readings
st.subheader("Abnormal readings")
a1, a2 = st.columns([3, 2])
anoms = q("""SELECT event_ts, sensor, value, z_score FROM sensor_lake.anomalies
              WHERE machine_id = ? AND event_date BETWEEN ? AND ?""", (machine, start, end))
fig = px.scatter(anoms, x="event_ts", y="z_score", color="sensor",
                 color_discrete_sequence=SERIES,
                 category_orders={"sensor": ["temperature_c", "vibration_mm_s", "pressure_bar"]},
                 hover_data={"value": ":.2f", "z_score": ":.1f"})
fig.update_traces(marker=dict(size=8, line=dict(width=1, color="#fcfcfb")))
fig.add_hline(y=3, line=dict(color=MUTED, dash="dot", width=1))
fig.add_hline(y=-3, line=dict(color=MUTED, dash="dot", width=1))
fig.update_layout(title=f"{machine}: point anomalies (|z| > 3, dotted lines)", hovermode="closest",
                  xaxis_title=None, yaxis_title="z-score")
a1.plotly_chart(style(fig, 360), use_container_width=True)

alerts = q("""SELECT machine_id, sensor, direction, alert_start, duration_min, anomaly_count,
                      ROUND(peak_abs_z, 1) AS peak_abs_z
               FROM sensor_lake.alerts
               WHERE event_date BETWEEN ? AND ?
               ORDER BY duration_min DESC""", (start, end))
if plant != "All":
    alerts = alerts[alerts.machine_id.isin(sel.machine_id.unique())]
a2.markdown("**Alert events** (persistent anomalies), longest first")
a2.dataframe(alerts, hide_index=True, height=330, use_container_width=True)

# ------------------------------------------------------------ machine-wise counts
st.subheader("Machine-wise anomaly counts")
per = (sel.groupby("machine_id", as_index=False)
       .agg(alert_events=("alert_count", "sum"), point_anomalies=("anomaly_count", "sum"))
       .sort_values(["alert_events", "point_anomalies"], ascending=False).head(15))
b1, b2 = st.columns(2)
fig = px.bar(per, x="machine_id", y="alert_events", color_discrete_sequence=[SERIES[0]])
fig.update_traces(marker_line_width=0)
fig.update_layout(title="Alert events: top 15 machines", bargap=0.35, hovermode="closest")
b1.plotly_chart(style(fig), use_container_width=True)
fig = px.bar(per, x="machine_id", y="point_anomalies", color_discrete_sequence=[SERIES[2]])
fig.update_traces(marker_line_width=0)
fig.update_layout(title="Point anomalies (3σ) for the same machines", bargap=0.35,
                  hovermode="closest")
b2.plotly_chart(style(fig), use_container_width=True)

# ------------------------------------------------------------ daily performance
st.subheader("Daily machine performance")
st.dataframe(sel.sort_values(["event_date", "machine_id"])
             .assign(health_status=lambda d: d.health_status.map(STATUS_ICON))
             [["event_date", "machine_id", "machine_type", "health_status", "uptime_pct",
               "avg_temperature_c", "max_temperature_c", "avg_vibration_mm_s",
               "max_vibration_mm_s", "min_pressure_bar", "alert_count", "alert_minutes",
               "anomaly_count", "invalid_readings"]],
             hide_index=True, use_container_width=True)
