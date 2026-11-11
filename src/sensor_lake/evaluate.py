"""Evaluate the anomaly detector against the generator's ground truth.

Because the data is synthetic we KNOW where the faults are, so we can
measure detector quality instead of eyeballing it:

  * precision     : share of flagged anomaly rows that fall inside a real
                    fault episode or on an injected spike
  * episode recall: share of fault episodes with >= 1 detection, and how
                    long after the episode began the first detection came
  * spike recall  : share of single-sample spikes detected
  * false positives by hour of day (to find systematic causes)

    python -m sensor_lake.evaluate
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from .config import PROJECT_ROOT, LakePaths, default_lake_root


def label_anomalies(anoms: pd.DataFrame, truth: pd.DataFrame) -> pd.Series:
    """True where an anomaly row lies inside a ground-truth episode/spike
    for the same machine and sensor."""
    hit = np.zeros(len(anoms), dtype=bool)
    a = anoms.reset_index(drop=True)
    for (m, s), ep in truth.groupby(["machine_id", "sensor"]):
        idx = np.flatnonzero((a["machine_id"] == m).to_numpy() & (a["sensor"] == s).to_numpy())
        if not len(idx):
            continue
        t = a.loc[idx, "epoch_s"].to_numpy()
        inside = np.zeros(len(idx), dtype=bool)
        for start, end in ep[["start_epoch", "end_epoch"]].to_numpy():
            inside |= (t >= start) & (t <= end)
        hit[idx] = inside
    return pd.Series(hit, index=anoms.index)


def evaluate(anoms: pd.DataFrame, truth: pd.DataFrame) -> dict:
    anoms = anoms.copy()
    anoms["is_true"] = label_anomalies(anoms, truth).to_numpy()
    episodes = truth[truth.fault_type != "spike"]
    spikes = truth[truth.fault_type == "spike"]

    ep_rows = []
    for e in episodes.itertuples():
        d = anoms[(anoms.machine_id == e.machine_id) & (anoms.sensor == e.sensor)
                  & (anoms.epoch_s >= e.start_epoch) & (anoms.epoch_s <= e.end_epoch)]
        ep_rows.append({"machine_id": e.machine_id, "fault_type": e.fault_type, "sensor": e.sensor,
                        "duration_min": round((e.end_epoch - e.start_epoch) / 60, 1),
                        "detected": len(d) > 0,
                        "first_detection_after_min":
                            round((d.epoch_s.min() - e.start_epoch) / 60, 1) if len(d) else None,
                        "anomaly_rows": len(d)})
    ep_df = pd.DataFrame(ep_rows)

    key = set(zip(anoms.machine_id, anoms.sensor, anoms.epoch_s))
    spike_hits = sum((m, s, t) in key for m, s, t in
                     zip(spikes.machine_id, spikes.sensor, spikes.start_epoch))
    fp = anoms[~anoms.is_true]
    fp_hour = (pd.to_datetime(fp.epoch_s, unit="s").dt.hour.value_counts().sort_index())
    return {
        "anomaly_rows": int(len(anoms)),
        "true_positive_rows": int(anoms.is_true.sum()),
        "precision_pct": round(100 * anoms.is_true.mean(), 2) if len(anoms) else None,
        "false_positive_rows": int(len(fp)),
        "episodes": int(len(ep_df)),
        "episode_recall_pct": round(100 * ep_df.detected.mean(), 1) if len(ep_df) else None,
        "median_detection_delay_min": float(ep_df.first_detection_after_min.median()),
        "spikes": int(len(spikes)), "spikes_detected": int(spike_hits),
        "spike_recall_pct": round(100 * spike_hits / max(len(spikes), 1), 1),
        "false_positives_by_hour_utc": {int(k): int(v) for k, v in fp_hour.items()},
        "false_positives_by_sensor": fp.sensor.value_counts().to_dict(),
        "episode_detail": ep_df.to_dict(orient="records"),
    }


def evaluate_alerts(alerts: pd.DataFrame, truth: pd.DataFrame) -> dict:
    """An alert is TRUE if it overlaps a fault episode (same machine+sensor)."""
    episodes = truth[truth.fault_type != "spike"]
    def overlaps(a) -> bool:
        e = episodes[(episodes.machine_id == a.machine_id) & (episodes.sensor == a.sensor)]
        return bool(((e.start_epoch <= a.end_epoch) & (e.end_epoch >= a.start_epoch)).any())
    alerts = alerts.copy()
    alerts["is_true"] = [overlaps(a) for a in alerts.itertuples()]
    detected = 0
    for e in episodes.itertuples():
        hit = alerts[(alerts.machine_id == e.machine_id) & (alerts.sensor == e.sensor)
                     & (alerts.start_epoch <= e.end_epoch) & (alerts.end_epoch >= e.start_epoch)]
        detected += len(hit) > 0
    false = alerts[~alerts.is_true]
    return {"alerts": int(len(alerts)), "true_alerts": int(alerts.is_true.sum()),
            "alert_precision_pct": round(100 * alerts.is_true.mean(), 1) if len(alerts) else None,
            "episodes_alerted": detected, "episodes": int(len(episodes)),
            "alert_episode_recall_pct": round(100 * detected / max(len(episodes), 1), 1),
            "false_alerts_by_sensor": false.sensor.value_counts().to_dict(),
            "false_alert_max_duration_min": float(false.duration_min.max()) if len(false) else 0.0}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake-root", default=default_lake_root())
    ap.add_argument("--truth", type=Path, default=PROJECT_ROOT / "ground_truth" / "fault_episodes.csv")
    ap.add_argument("--out", type=Path, default=PROJECT_ROOT / "sample_output" / "anomaly_evaluation.json")
    a = ap.parse_args()
    anoms = pd.read_parquet(LakePaths(a.lake_root).curated_anomalies,
                            columns=["machine_id", "sensor", "epoch_s", "z_score"])
    truth = pd.read_csv(a.truth)
    result = evaluate(anoms, truth)
    alerts = pd.read_parquet(LakePaths(a.lake_root).curated_alerts)
    result["alert_evaluation"] = evaluate_alerts(alerts, truth)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(result, indent=2, default=str))
    print(json.dumps({k: v for k, v in result.items() if k != "episode_detail"}, indent=1))
    print(pd.DataFrame(result["episode_detail"]).to_string(index=False))


if __name__ == "__main__":
    main()
