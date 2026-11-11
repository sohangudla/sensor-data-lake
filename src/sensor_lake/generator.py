"""Synthetic manufacturing sensor data generator.

Simulates `machines` machines, each reporting temperature, vibration and
pressure every READING_INTERVAL_S seconds, and lands the readings in the RAW
zone as gzipped JSON-lines files, one file per hour:

    raw/sensor_readings/ingest_date=2026-11-01/readings_2026110100.json.gz

Signal model per sensor (think of it as an ECE signal chain):
    value = baseline + daily cycle (ambient temp) + shift-load effect
            + Gaussian noise                                  (normal operation)
            + injected fault episodes                         (ground truth)

Data problems deliberately injected (what the pipeline must handle):
    * missing values      : random single-sensor dropouts (NULL)
    * offline gaps        : some machines send nothing for 20-90 minutes
    * duplicates          : ~0.5% of readings re-transmitted, landing in the NEXT
                            hour's file (so dedup must work across files)
    * impossible values   : sentinel values like -999 / 9999 from faulty sensors
    * corrupt records     : a few truncated JSON lines
    * abnormal readings   : bearing wear (vibration ramp), overheating,
                            pressure leak, and single-sample EMI spikes

The injected fault episodes are written to ground_truth/fault_episodes.csv so
the anomaly detector can be evaluated (it is NOT part of the lake).
"""
from __future__ import annotations

import argparse
import gzip
import json
import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from .config import READING_INTERVAL_S, PROJECT_ROOT

log = logging.getLogger(__name__)

MACHINE_TYPES = {
    #               temp_c  vib_mm_s  pressure_bar
    "pump":        (55.0,   2.0,      6.0),
    "compressor":  (70.0,   3.0,      8.0),
    "conveyor":    (45.0,   1.5,      4.5),
    "cnc_mill":    (60.0,   2.5,      5.5),
}
NOISE_SIGMA = {"temperature_c": 0.6, "vibration_mm_s": 0.12, "pressure_bar": 0.06}


@dataclass
class GeneratorConfig:
    machines: int = 50
    days: int = 7
    start: str = "2026-11-01 00:00:00"
    interval_s: int = READING_INTERVAL_S
    seed: int = 42
    missing_rate: float = 0.01
    duplicate_rate: float = 0.005
    sentinel_rate: float = 0.0002
    spike_rate: float = 0.0003
    corrupt_rate: float = 0.00005


def machine_catalog(n: int, rng: np.random.Generator) -> pd.DataFrame:
    types = list(MACHINE_TYPES)
    rows = []
    for i in range(1, n + 1):
        mtype = types[(i - 1) % len(types)]
        rows.append({
            "machine_id": f"M{i:03d}",
            "machine_type": mtype,
            "plant": "PLANT-A" if i <= n // 2 else "PLANT-B",
            "production_line": f"LINE-{(i - 1) % 5 + 1}",
            "install_year": int(rng.integers(2012, 2025)),
        })
    return pd.DataFrame(rows)


def _ramp(n_points: int, peak: float) -> np.ndarray:
    return np.linspace(0.0, peak, n_points)


def simulate_machine(machine: pd.Series, epoch: np.ndarray, cfg: GeneratorConfig,
                     rng: np.random.Generator) -> tuple[pd.DataFrame, list[dict]]:
    n = len(epoch)
    base_t, base_v, base_p = MACHINE_TYPES[machine.machine_type]
    # each machine is slightly different
    base_t += rng.normal(0, 2.0)
    base_v *= rng.uniform(0.85, 1.15)
    base_p += rng.normal(0, 0.3)

    seconds_of_day = epoch % 86400
    hour = seconds_of_day / 3600
    daily = np.sin(2 * np.pi * (seconds_of_day - 6 * 3600) / 86400)   # peak mid-afternoon
    on_shift = ((hour >= 6) & (hour < 22)).astype(float)               # 2-shift operation

    temp = base_t + 3.0 * daily + 4.0 * on_shift + rng.normal(0, NOISE_SIGMA["temperature_c"], n)
    vib = base_v * (1 + 0.15 * on_shift) + rng.normal(0, NOISE_SIGMA["vibration_mm_s"], n)
    pres = base_p + 0.3 * on_shift + rng.normal(0, NOISE_SIGMA["pressure_bar"], n)

    episodes: list[dict] = []

    def add_episode(fault: str, sensor: str, arr: np.ndarray, duration_min: int, peak: float,
                    hold_min: int = 0) -> None:
        ramp_pts = duration_min * 60 // cfg.interval_s
        hold_pts = hold_min * 60 // cfg.interval_s
        start = int(rng.integers(n // 10, n - ramp_pts - hold_pts - 1))
        arr[start:start + ramp_pts] += _ramp(ramp_pts, peak)
        arr[start + ramp_pts:start + ramp_pts + hold_pts] += peak
        end = start + ramp_pts + hold_pts - 1
        episodes.append({"machine_id": machine.machine_id, "fault_type": fault, "sensor": sensor,
                         "start_epoch": int(epoch[start]), "end_epoch": int(epoch[end])})

    mid = int(machine.machine_id[1:])
    if mid % 8 == 3:        # ~6 machines: bearing wear -> vibration climbs
        add_episode("bearing_wear", "vibration_mm_s", vib, 45, float(rng.uniform(8, 12)), hold_min=15)
    if mid % 12 == 5:       # ~4 machines: cooling failure -> overheating
        add_episode("overheating", "temperature_c", temp, 30, float(rng.uniform(20, 30)), hold_min=20)
    if mid % 16 == 7:       # ~3 machines: hydraulic leak -> sudden pressure drop
        pts = 15 * 60 // cfg.interval_s
        start = int(rng.integers(n // 10, n - pts - 1))
        pres[start:start + pts] -= float(rng.uniform(2.5, 3.5))
        episodes.append({"machine_id": machine.machine_id, "fault_type": "pressure_leak",
                         "sensor": "pressure_bar", "start_epoch": int(epoch[start]),
                         "end_epoch": int(epoch[start + pts - 1])})

    df = pd.DataFrame({
        "machine_id": machine.machine_id,
        "epoch": epoch,
        "temperature_c": temp.round(2),
        "vibration_mm_s": np.abs(vib).round(3),
        "pressure_bar": np.clip(pres, 0, None).round(3),
    })

    # EMI spikes: single-sample glitches of 8-15 sigma
    for sensor, arr_col in (("temperature_c", "temperature_c"), ("vibration_mm_s", "vibration_mm_s"),
                            ("pressure_bar", "pressure_bar")):
        idx = np.flatnonzero(rng.random(n) < cfg.spike_rate / 3)
        sign = 1 if sensor != "pressure_bar" else rng.choice([-1, 1], len(idx))
        df.loc[idx, arr_col] += sign * rng.uniform(8, 15, len(idx)) * NOISE_SIGMA[sensor]
        for k in idx:
            episodes.append({"machine_id": machine.machine_id, "fault_type": "spike", "sensor": sensor,
                             "start_epoch": int(epoch[k]), "end_epoch": int(epoch[k])})
    df["vibration_mm_s"] = df["vibration_mm_s"].abs()
    df["pressure_bar"] = df["pressure_bar"].clip(lower=0)

    # offline gap: the machine stops reporting entirely
    if mid % 6 == 1:
        gap = int(rng.integers(20, 91)) * 60 // cfg.interval_s
        g0 = int(rng.integers(0, n - gap))
        df = df.drop(df.index[g0:g0 + gap])

    # random single-sensor dropouts -> NULL
    for sensor in NOISE_SIGMA:
        mask = rng.random(len(df)) < cfg.missing_rate / 3
        df.loc[mask, sensor] = np.nan
    # impossible sentinel values from a faulty sensor
    for sensor in NOISE_SIGMA:
        mask = rng.random(len(df)) < cfg.sentinel_rate / 3
        df.loc[mask, sensor] = rng.choice([-999.0, 9999.0], mask.sum())
    return df, episodes


def generate(out_root: Path, cfg: GeneratorConfig, truth_dir: Path) -> dict:
    rng = np.random.default_rng(cfg.seed)
    t0 = int(pd.Timestamp(cfg.start, tz="UTC").timestamp())
    epoch = np.arange(t0, t0 + cfg.days * 86400, cfg.interval_s, dtype=np.int64)
    catalog = machine_catalog(cfg.machines, rng)

    frames, episodes = [], []
    for _, m in catalog.iterrows():
        df, ep = simulate_machine(m, epoch, cfg, rng)
        frames.append(df)
        episodes += ep
    readings = pd.concat(frames, ignore_index=True)
    n_clean = len(readings)

    # duplicates (re-transmissions), delivered with the next hour's file
    dup = readings.sample(frac=cfg.duplicate_rate, random_state=cfg.seed).copy()
    readings["file_hour"] = readings["epoch"] // 3600
    dup["file_hour"] = dup["epoch"] // 3600 + 1
    readings = pd.concat([readings, dup], ignore_index=True)

    readings = readings.merge(catalog[["machine_id", "machine_type", "plant"]], on="machine_id")
    readings["reading_id"] = readings["machine_id"] + "-" + readings["epoch"].astype(str)
    readings = readings.sort_values(["file_hour", "epoch", "machine_id"])

    raw_dir = out_root / "raw" / "sensor_readings"
    n_corrupt = n_written = files = 0
    for file_hour, chunk in readings.groupby("file_hour", sort=True):
        ts = pd.to_datetime(file_hour * 3600, unit="s", utc=True)
        part = raw_dir / f"ingest_date={ts:%Y-%m-%d}"
        part.mkdir(parents=True, exist_ok=True)
        out = chunk[["reading_id", "machine_id", "machine_type", "plant", "epoch",
                     "temperature_c", "vibration_mm_s", "pressure_bar"]].copy()
        out.insert(4, "event_ts", pd.to_datetime(out.pop("epoch"), unit="s", utc=True)
                   .dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
        out["firmware"] = "fw-2.4.1"
        lines = out.to_json(orient="records", lines=True).splitlines()
        corrupt_idx = np.flatnonzero(rng.random(len(lines)) < cfg.corrupt_rate)
        for k in corrupt_idx:
            lines[k] = lines[k][: len(lines[k]) // 2]        # truncated transmission
        n_corrupt += len(corrupt_idx)
        with gzip.open(part / f"readings_{ts:%Y%m%d%H}.json.gz", "wt", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
        n_written += len(lines)
        files += 1

    ref_dir = out_root / "raw" / "reference" / "machines"
    ref_dir.mkdir(parents=True, exist_ok=True)
    catalog.to_json(ref_dir / "machines.json", orient="records", lines=True)

    truth_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(episodes).to_csv(truth_dir / "fault_episodes.csv", index=False)

    stats = {"machines": cfg.machines, "days": cfg.days, "interval_s": cfg.interval_s,
             "unique_readings": n_clean, "duplicates_injected": len(dup),
             "corrupt_lines_injected": n_corrupt, "lines_written": n_written, "files": files,
             "fault_episodes": sum(e["fault_type"] != "spike" for e in episodes),
             "spikes": sum(e["fault_type"] == "spike" for e in episodes)}
    (truth_dir / "generator_stats.json").write_text(json.dumps(stats, indent=2))
    return stats


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    ap = argparse.ArgumentParser(description="Generate synthetic sensor data into the raw zone")
    ap.add_argument("--out", type=Path, default=PROJECT_ROOT / "lake")
    ap.add_argument("--truth", type=Path, default=PROJECT_ROOT / "ground_truth")
    ap.add_argument("--machines", type=int, default=50)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--start", default="2026-11-01 00:00:00")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    stats = generate(a.out, GeneratorConfig(machines=a.machines, days=a.days, start=a.start,
                                            seed=a.seed), a.truth)
    log.info("Generated: %s", stats)


if __name__ == "__main__":
    main()
