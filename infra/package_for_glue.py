"""Zip the sensor_lake package for Glue's --extra-py-files (build/sensor_lake.zip).
Glue only needs pure-Python code; Spark and boto3 are provided by Glue."""
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "build" / "sensor_lake.zip"
SKIP = {"generator.py", "local_sql.py", "evaluate.py"}   # local-only modules (need numpy/duckdb)

OUT.parent.mkdir(exist_ok=True)
with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED) as z:
    for f in sorted((ROOT / "src" / "sensor_lake").glob("*.py")):
        if f.name not in SKIP:
            z.write(f, f"sensor_lake/{f.name}")
print(f"Wrote {OUT} ({OUT.stat().st_size} bytes):", zipfile.ZipFile(OUT).namelist())
