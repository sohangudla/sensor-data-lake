"""Generate a small lake, run the real pipeline twice, check the outputs."""
from sensor_lake import pipeline
from sensor_lake.config import LakePaths
from sensor_lake.generator import GeneratorConfig, generate


def test_pipeline_end_to_end_and_idempotent(spark, tmp_path):
    lake = tmp_path / "lake"
    stats = generate(lake, GeneratorConfig(machines=3, days=1, seed=11), tmp_path / "truth")

    s1 = pipeline.run(spark, lake.as_posix(), publish=False)
    m = s1["metrics"]
    assert m["ValidationFailures"] == 0
    assert m["RawRows"] == stats["lines_written"]
    assert m["CorruptRows"] == stats["corrupt_lines_injected"]
    # every raw row is accounted for
    assert m["RawRows"] == m["CorruptRows"] + m["MissingKeyRows"] + m["DuplicatesRemoved"] + m["CleanRows"]
    assert m["DailyRows"] == 3

    s2 = pipeline.run(spark, lake.as_posix(), publish=False)        # rerun
    assert s2["metrics"]["CleanRows"] == m["CleanRows"]              # no duplication
    assert s2["metrics"]["AnomalyRows"] == m["AnomalyRows"]
    p = LakePaths(lake.as_posix())
    assert spark.read.parquet(p.clean_readings).count() == m["CleanRows"]
