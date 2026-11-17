"""Tests unitarios de las transformaciones (pytest). Corren en Spark local sobre DataFrames chicos."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pyspark.sql import SparkSession, functions as F  # noqa: E402

from datalake import transforms as T  # noqa: E402
from datalake.quality import apply_row_rules, split_valid_quarantine  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").config("spark.sql.shuffle.partitions", "1").getOrCreate()
    yield s
    s.stop()


def raw_titles(spark, rows):
    cols = ["tconst", "titleType", "primaryTitle", "originalTitle", "isAdult", "startYear", "endYear", "runtimeMinutes", "genres"]
    schema = ", ".join(f"{c} string" for c in cols)
    return spark.createDataFrame(rows, schema).withColumn("_ingest_ts", F.current_timestamp()).withColumn("_source_file", F.lit("f")).withColumn("_corrupt_record", F.lit(None).cast("string"))


def test_title_cleaning_sends_bad_rows_to_quarantine(spark):
    df = raw_titles(spark, [
        ("tt0000001", "movie", "Ok", "Ok", "0", "2010", None, "120", "Drama,Comedy"),
        ("tt0000002", "movie", "Bad year", "x", "0", "1700", None, "100", "Drama"),
        ("tt0000003", "movie", "Bad runtime", "x", "0", "2010", None, "9999", "Drama"),
        ("xx123", "movie", "Bad id", "x", "0", "2010", None, "90", None),
    ])
    checked = apply_row_rules(T.type_title_basics(df), T.title_basics_rules(1874, 2031, 600))
    valid, quarantine = split_valid_quarantine(checked)
    assert [r.tconst for r in valid.collect()] == ["tt0000001"]
    errors = {r.tconst: r._dq_errors for r in quarantine.collect()}
    assert errors["tt0000002"] == ["startYear_range"]
    assert errors["tt0000003"] == ["runtime_range"]
    assert "tconst_format" in errors["xx123"]


def test_genres_are_split_and_exploded(spark):
    df = raw_titles(spark, [("tt0000001", "movie", "A", "A", "0", "2010", None, "120", "Drama,Comedy")])
    titles = T.finalize_title_basics(T.type_title_basics(df))
    assert titles.first().genres == ["Drama", "Comedy"]
    assert titles.first().decade == 2010
    assert sorted(r.genre for r in T.explode_genres(titles).collect()) == ["Comedy", "Drama"]


def test_deduplicate_keeps_latest(spark):
    df = spark.createDataFrame([("tt1", 1, "a"), ("tt1", 2, "b")], ["tconst", "_ingest_ts", "_source_file"])
    assert T.deduplicate(df, ["tconst"]).first()._source_file == "b"


def test_weighted_rating_shrinks_low_vote_titles(spark):
    df = spark.createDataFrame([("few", 9.5, 40), ("many", 8.6, 2_000_000)], ["t", "averageRating", "numVotes"])
    wr = {r.t: r.weighted_rating for r in T.weighted_rating(df, m=5000, c=6.4).collect()}
    assert wr["many"] > wr["few"]
    assert 6.4 < wr["few"] < 6.5


def test_sessions_per_day(spark):
    df = spark.createDataFrame([(90,), (180,)], ["runtimeMinutes"])
    out = [r.s for r in df.select(T.sessions_per_day("runtimeMinutes", 840, 30).alias("s")).collect()]
    assert out == [7, 4]  # 840/120 = 7 ; 840/210 = 4
