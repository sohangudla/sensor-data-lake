"""Compile WindowsLocalFileSystem.java into windows_compat/windows-localfs.jar
against the Hadoop client jars bundled with PySpark. Needs a JDK (javac, jar)."""
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import pyspark

HERE = Path(__file__).resolve().parent
JAR = HERE / "windows-localfs.jar"


def tool(name: str) -> str:
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = Path(java_home) / "bin" / (name + (".exe" if os.name == "nt" else ""))
        if candidate.exists():
            return str(candidate)
    found = shutil.which(name)
    if not found:
        raise SystemExit(f"{name} not found: install a JDK and set JAVA_HOME")
    return found


def build() -> Path:
    jars = Path(pyspark.__file__).parent / "jars"
    cp = os.pathsep.join(str(p) for p in jars.glob("hadoop-client-*.jar"))
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run([tool("javac"), "--release", "8", "-nowarn", "-cp", cp, "-d", tmp,
                        str(HERE / "src" / "WindowsLocalFileSystem.java")], check=True)
        subprocess.run([tool("jar"), "cf", str(JAR), "-C", tmp, "."], check=True)
    print(f"Built {JAR}")
    return JAR


if __name__ == "__main__":
    build()
