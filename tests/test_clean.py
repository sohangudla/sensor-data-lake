import gzip

from pyspark.sql import Row

from sensor_lake import clean as C


def raw_df(spark, rows):
    cols = "reading_id string, machine_id string, machine_type string, plant string, " \
           "event_ts string, temperature_c double, vibration_mm_s double, pressure_bar double, " \
           "firmware string, ingest_date string, source_file string"
    return spark.createDataFrame(rows, cols)


def r(machine="M001", ts="2026-11-02T12:00:00Z", t=60.0, v=2.0, p=6.0, ingest="2026-11-02", f="a"):
    return (f"{machine}-{ts}", machine, "pump", "PLANT-A", ts, t, v, p, "fw", ingest, f)


def test_corrupt_json_lines_are_separated(spark, tmp_path):
    d = tmp_path / "raw" / "ingest_date=2026-11-02"
    d.mkdir(parents=True)
    with gzip.open(d / "x.json.gz", "wt") as fh:
        fh.write('{"machine_id":"M001","event_ts":"2026-11-02T12:00:00Z","temperature_c":60.1}\n')
        fh.write('{"machine_id":"M001","event_ts":"2026-11-0\n')                     # truncated
    raw = C.read_raw_readings(spark, str(tmp_path / "raw")).cache()
    good, corrupt = C.split_corrupt(raw)
    assert good.count() == 1 and corrupt.count() == 1
    assert good.first()["ingest_date"].isoformat() == "2026-11-02"   # partition discovery


def test_unparseable_timestamp_goes_to_quarantine(spark):
    ok, bad = C.parse_keys(raw_df(spark, [r(), r(ts="02/11/2026 12:00"), r(machine=None)]))
    assert ok.count() == 1 and bad.count() == 2


def test_deduplicate_keeps_latest_ingest(spark):
    df = raw_df(spark, [r(t=60.0, ingest="2026-11-02"), r(t=61.0, ingest="2026-11-03"),
                        r(ts="2026-11-02T12:00:05Z")])
    keyed, _ = C.parse_keys(df)
    out = C.deduplicate(keyed).orderBy("event_ts").collect()
    assert len(out) == 2
    assert out[0]["temperature_c"] == 61.0


def test_validate_ranges_nulls_impossible_and_flags_out_of_spec(spark):
    keyed, _ = C.parse_keys(raw_df(spark, [r(t=-999.0), r(ts="2026-11-02T12:00:05Z", t=95.0),
                                           r(ts="2026-11-02T12:00:10Z", p=2.0)]))
    rows = C.validate_ranges(keyed).orderBy("event_ts").collect()
    assert rows[0]["temperature_c"] is None and rows[0]["temperature_c_invalid"]
    assert rows[1]["temperature_c"] == 95.0 and rows[1]["temperature_c_out_of_spec"]
    assert not rows[1]["temperature_c_invalid"]
    assert rows[2]["pressure_bar_out_of_spec"] and not rows[2]["temperature_c_out_of_spec"]


def test_fill_short_gaps_only_within_max_age(spark):
    df = raw_df(spark, [r(ts="2026-11-02T12:00:00Z", t=60.0),
                        r(ts="2026-11-02T12:00:05Z", t=None),     # 5 s gap  -> filled
                        r(ts="2026-11-02T12:00:30Z", t=None)])    # 30 s gap -> stays NULL
    keyed, _ = C.parse_keys(df)
    rows = C.fill_short_gaps(C.validate_ranges(keyed)).orderBy("event_ts").collect()
    assert rows[1]["temperature_c"] == 60.0 and rows[1]["temperature_c_imputed"]
    assert rows[2]["temperature_c"] is None and not rows[2]["temperature_c_imputed"]


def test_clean_readings_output_columns(spark):
    clean, bad = C.clean_readings(raw_df(spark, [r(), r()]))   # exact duplicate
    assert clean.count() == 1 and bad.count() == 0
    assert clean.columns == C.CLEAN_COLUMNS
    assert clean.first()["event_hour"] == 12
