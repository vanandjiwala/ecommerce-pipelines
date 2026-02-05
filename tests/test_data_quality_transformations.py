from utils.transformation_utils import *
import pytest
from pyspark.testing import assertDataFrameEqual
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from datetime import datetime
import pyspark.sql.functions as F

@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-spark")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()

@pytest.fixture
def df_with_duplicates(spark):
    data = [
        Row(id=1, value="A", ingest_ts=10),
        Row(id=1, value="B", ingest_ts=20),
    ]
    df = spark.createDataFrame(data)
    return df

@pytest.fixture
def df_with_duplicate_by_partitions(spark):
    data = [
        Row(id=1, region="US", value="A", ingest_ts=10),
        Row(id=1, region="US", value="B", ingest_ts=20),
        Row(id=1, region="EU", value="C", ingest_ts=30),
    ]
    df = spark.createDataFrame(data)
    return df

@pytest.fixture
def df_with_duplicate_by_ts(spark):
    data = [
        Row(id=1, region="US", value="A", ingest_ts=10),
        Row(id=1, region="US", value="B", ingest_ts=10),
        Row(id=1, region="EU", value="C", ingest_ts=30),
    ]
    df = spark.createDataFrame(data)
    return df

@pytest.fixture
def df_with_duplicate_by_null_ts(spark):
    data = [
        Row(id=1, region="US", value="A", ingest_ts=None),
        Row(id=1, region="US", value="B", ingest_ts=10)
    ]
    df = spark.createDataFrame(data)
    return df

@pytest.fixture
def df_with_no_records(spark):
    data = []
    df = spark.createDataFrame(data, "id INT, value STRING, ingest_ts INT")
    return df


def test_deduped_df(spark, df_with_duplicates):
    deduped_df = deduplicate_data_by_time(
        spark,
        df_with_duplicates,
        partition_cols=["id"],
        order_col="ingest_ts"
    )
    rows = deduped_df.count()
    assert rows == 1
    assert deduped_df.collect()[0]["value"] == "B"

def test_deduped_partition_df(spark, df_with_duplicate_by_partitions):
    deduped_df = deduplicate_data_by_time(
        spark,
        df_with_duplicate_by_partitions,
        partition_cols=["id", "region"],
        order_col="ingest_ts"
    )
    rows = deduped_df.count()
    assert rows == 2

    expected_data = [
        Row(id=1, region="US", value="B", ingest_ts=20),
        Row(id=1, region="EU", value="C", ingest_ts=30),
    ]
    expected_df = spark.createDataFrame(expected_data)
    assertDataFrameEqual(deduped_df,expected_df)

def test_deduped_ts_df(spark, df_with_duplicate_by_ts):
    deduped_df = deduplicate_data_by_time(
        spark,
        df_with_duplicate_by_ts,
        partition_cols=["id", "region"],
        order_col="ingest_ts"
    )
    rows = deduped_df.count()
    assert rows == 2

def test_deduped_null_ts_df(spark, df_with_duplicate_by_null_ts):
    deduped_df = deduplicate_data_by_time(
        spark,
        df_with_duplicate_by_null_ts,
        partition_cols=["id", "region"],
        order_col="ingest_ts"
    )
    rows = deduped_df.count()
    assert rows == 1

    row = deduped_df.collect()[0]
    assert row["value"] == "B"

def test_deduped_empty_df(spark, df_with_no_records):
    deduped_df = deduplicate_data_by_time(
        spark,
        df_with_no_records,
        partition_cols=["id"],
        order_col="ingest_ts"
    )
    rows = deduped_df.count()
    assert rows == 0

    