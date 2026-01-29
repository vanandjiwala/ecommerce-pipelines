from utils import dwh_utils
from utils.transformation_utils import *
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from datetime import datetime
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('integrity-tests').getOrCreate()


def test_normalize_column_names():
    data = [(1, "Alice", 100), (2, "Bob", 200)]
    df = spark.createDataFrame(data, ["User ID", "First Name", "Total Amount"])

    result = normalize_column_names(spark, df)
    
    expected_columns = ["user_id", "first_name", "total_amount"]
    assert result.columns == expected_columns

def test_normalize_column_names_already_normalized():
    data = [(1, 2)]
    df = spark.createDataFrame(data, ["col_a", "col_b"])

    result = normalize_column_names(spark, df)

    expected_columns = ["col_a", "col_b"]
    assert result.columns == expected_columns

def test_drop_columns():
    data = [(1, 2), (3, 4)]
    df = spark.createDataFrame(data, ["col_a", "col_b"])

    result = drop_column(spark=spark, df=df, column="col_a")

    expected_columns = ["col_b"]
    assert result.columns == expected_columns

def test_drop_columns_length():
    data = [(1, 2), (3, 4)]
    df = spark.createDataFrame(data, ["col_a", "col_b"])

    result = drop_column(spark=spark, df=df, column="col_a")

    assert len(result.columns) == 1

def test_drop_columns_df_rows():
    data = [(1, 2), (3, 4)]
    df = spark.createDataFrame(data, ["col_a", "col_b"])
    original_rows = df.count()

    result = drop_column(spark=spark, df=df, column="col_a")

    rows = df.count()
    assert original_rows == rows

def test_duplicate_by_time_with_duplicates():
    data = [
        (1, "pepsi", datetime(2024, 1, 15, 10, 30, 0)),
        (1, "pepsi", datetime(2024, 1, 16, 14, 45, 0)),
        (2, "coke", datetime(2024, 1, 15, 11, 0, 0)),
        (2, "coke", datetime(2024, 1, 18, 16, 30, 0)),
        (3, "chipotle", datetime(2024, 1, 15, 12, 0, 0))
    ]

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("ingest_timestamp", TimestampType(), False)
    ])

    df = spark.createDataFrame(data, schema)
    df =  deduplicate_data_by_time(spark, df, ["id"], "ingest_timestamp")
    assert df.count() == 3

def test_duplicate_by_time_without_duplicates():
    data = [
        (1, "pepsi", datetime(2024, 1, 15, 10, 30, 0)),
        (2, "coke", datetime(2024, 1, 15, 11, 0, 0)),
        (3, "chipotle", datetime(2024, 1, 15, 12, 0, 0))
    ]

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("ingest_timestamp", TimestampType(), False)
    ])

    df = spark.createDataFrame(data, schema)
    df =  deduplicate_data_by_time(spark, df, ["id"], "ingest_timestamp")
    assert df.count() == 3

def test_duplicate_by_time_latest_record():
    data = [
        (1, "pepsi", datetime(2024, 1, 15, 10, 30, 0)),
        (1, "coke", datetime(2024, 1, 18, 11, 0, 0))
    ]

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("ingest_timestamp", TimestampType(), False)
    ])

    df = spark.createDataFrame(data, schema)
    df =  deduplicate_data_by_time(spark, df, ["id"], "ingest_timestamp")

    row = df.collect()[0]
    assert row["ingest_timestamp"] == datetime(2024, 1, 18, 11, 0, 0)

def test_round_value_lower():
    data = [
        (1, "apple", 5.2511),
        (2, "google", 6.504),
        (2, "google", 7.547)
    ]

    df = spark.createDataFrame(data, ["id", "company", "price"])
    df = round_value(spark, df, "price", 2)

    assert df.collect()[0]["price"] == 5.25

def test_round_value_lower_negative():
    data = [
        (1, "apple", 5.2511),
        (2, "google", 6.504),
        (2, "google", 7.547)
    ]

    df = spark.createDataFrame(data, ["id", "company", "price"])
    df = round_value(spark, df, "price", 2)

    assert df.collect()[0]["price"] != 5.26

def test_round_value_upper():
    data = [
        (1, "apple", 5.2511),
        (2, "google", 6.559),
        (2, "google", 7.547)
    ]

    df = spark.createDataFrame(data, ["id", "company", "price"])
    df = round_value(spark, df, "price", 2)

    assert df.collect()[1]["price"] == 6.56

def test_round_value_upper_negative():
    data = [
        (1, "apple", 5.2511),
        (2, "google", 6.559),
        (2, "google", 7.547)
    ]

    df = spark.createDataFrame(data, ["id", "company", "price"])
    df = round_value(spark, df, "price", 2)

    assert df.collect()[1]["price"] != 6.55


def test_date_type_conversion():
    data = [
        (1, "2023/01/01"),
        (2, "2023/02/01"),
        (3, "2023/03/01")
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("ship_date", StringType(), False),
    ])

    df = spark.createDataFrame(data, schema)
    df = to_date_col(spark, df, "ship_date", "yyyy/MM/dd")

    assert dict(df.dtypes)["ship_date"] == "date"

def test_date_type_conversion_default_format():
    data = [
        (1, "01/01/2026"),
        (2, "01/02/2026"),
        (3, "01/03/2026")
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("ship_date", StringType(), False),
    ])

    df = spark.createDataFrame(data, schema)
    df = to_date_col(spark, df, "ship_date")

    assert dict(df.dtypes)["ship_date"] == "date"


def test_timestamp_type_conversion():
    data = [
        (1, "2023/01/01"),
        (2, "2023/02/01"),
        (3, "2023/03/01")
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("ship_date", StringType(), False),
    ])

    df = spark.createDataFrame(data, schema)
    df = to_timestamp_col(spark, df, "ship_date", "yyyy/MM/dd")

    assert dict(df.dtypes)["ship_date"] == "timestamp"

def test_timestamp_type_conversion_default_format():
    data = [
        (1, "01/01/2026"),
        (2, "01/02/2026"),
        (3, "01/03/2026")
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("ship_date", StringType(), False),
    ])

    df = spark.createDataFrame(data, schema)
    df = to_timestamp_col(spark, df, "ship_date")

    assert dict(df.dtypes)["ship_date"] == "timestamp"

def test_not_nulls():
    data = [
        (1, "prod1"),
        (2, "prod2"),
        (3, None)
    ]
    df = spark.createDataFrame(data, ["id", "name"])
    df = fill_nulls(spark, df, "name", "NA")
    null_count = df.filter(df["name"].isNull()).count()
    assert null_count == 0

def test_all_nulls():
    data = [
        (1),
        (2),
        (3)
    ]
    df = spark.createDataFrame(data, ["id"])
    df = df.withColumn("name", F.lit(None))
    df = fill_nulls(spark, df, "name", "NA")
    null_count = df.filter(df["name"].isNull()).count()
    assert null_count == 0