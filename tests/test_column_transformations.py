from utils.transformation_utils import *
import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType
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
def df_with_space_columns(spark):
    data = [(1, "Alice", 100), (2, "Bob", 200)]
    df = spark.createDataFrame(data, ["User ID", "First Name", "Total Amount"])
    return df

@pytest.fixture
def df_with_special_chars_columns(spark):
    df = spark.createDataFrame(
        [("2026-01-01", "placed")],
        ["Created@Date", "Order#Status"]
    )
    return df

@pytest.fixture
def df_with_camelcase_columns(spark):
    df = spark.createDataFrame(
        [("2026-01-01", "placed")],
        ["CreatedDate", "OrderStatus"]
    )
    return df

@pytest.fixture
def df_with_multi_special_chars_columns(spark):
    df = spark.createDataFrame(
        [(1, "2026-01-01", 500),
         (2, "2026-01-02", 1500),
         (3, "2026-01-03", 3500)],
        ["order-id", "order-date", "order-daily-sales-amount"]
    )
    return df

@pytest.fixture
def df_with_mixed_chars_columns(spark):
    df = spark.createDataFrame(
        [(1, "2026-01-01", 500),
         (2, "2026-01-02", 1500),
         (3, "2026-01-03", 3500)],
        ["order-id", "order_date", "order_daily-sales-amount"]
    )
    return df

@pytest.fixture
def df_with_normalized_columns(spark):
    df = spark.createDataFrame(
        [(1, "2026-01-01", 500),
         (2, "2026-01-02", 1500),
         (3, "2026-01-03", 3500)],
        ["order_id", "order_date", "order_daily_sales_amount"]
    )
    return df

@pytest.fixture
def df_dropped_columns(spark):
    data = [(1, 2), (3, 4)]
    df = spark.createDataFrame(data, ["col_a", "col_b"])
    return df

@pytest.fixture
def df_single_column(spark):
    data = [(1,), (2,)]
    df = spark.createDataFrame(data, ["col_a"])
    return df

@pytest.fixture
def df_null_column(spark):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])

    df = spark.createDataFrame(
        [
            (None, None),
            (1, None),
            (2, None)
        ],
        schema=schema
    )
    return df

@pytest.fixture
def df_basic_round_value(spark):
    df = spark.createDataFrame(
        [(1, 12.345), (2, 67.891)],
        ["id", "amount"]
    )
    return df

@pytest.fixture
def df_basic_round_negative_value(spark):
    df = spark.createDataFrame(
        [(1, -12.345), (2, -67.891)],
        ["id", "amount"]
    )
    return df

@pytest.fixture
def df_basic_round_null_value(spark):
    df = spark.createDataFrame(
        [(1, 7.392), (2, None)],
        ["id", "amount"]
    )
    return df

@pytest.fixture
def df_basic_round_int_value(spark):
    df = spark.createDataFrame(
        [(1, 7), (2, 3)],
        ["id", "amount"]
    )
    return df

@pytest.fixture
def df_date_col_default_format(spark):
    df = spark.createDataFrame(
        [("01/01/2026",), ("01/02/2026",)],
        ["dt"]
    )
    return df

@pytest.fixture
def df_date_col_custom_format(spark):
    df = spark.createDataFrame(
        [("2026-01-01",), ("2026-01-02",)],
        ["dt"]
    )
    return df

@pytest.fixture
def df_date_col_invalid_dates(spark):
    df = spark.createDataFrame(
        [("2026-50-34",), ("2026-100-02",)],
        ["dt"]
    )
    return df

def test_normalize_column_with_space(spark, df_with_space_columns):
    result = normalize_column_names(spark = spark, df = df_with_space_columns)
    expected_columns = ["user_id", "first_name", "total_amount"]
    assert result.columns == expected_columns

def test_normalize_column_with_special_chars(spark, df_with_special_chars_columns):
    result = normalize_column_names(spark = spark, df = df_with_special_chars_columns)
    expected_columns = ["created_date", "order_status"]
    assert result.columns == expected_columns

def test_normalize_column_with_camelcase_chars(spark, df_with_camelcase_columns):
    result = normalize_column_names(spark = spark, df = df_with_camelcase_columns)
    expected_columns = ["createddate", "orderstatus"]
    assert result.columns == expected_columns

def test_normalize_column_with_multi_special_chars(spark, df_with_multi_special_chars_columns):
    result = normalize_column_names(spark = spark, df = df_with_multi_special_chars_columns)
    expected_columns = ["order_id", "order_date", "order_daily_sales_amount"]
    assert result.columns == expected_columns

def test_normalize_column_with_normalized_cols(spark, df_with_normalized_columns):
    result = normalize_column_names(spark = spark, df = df_with_normalized_columns)
    expected_columns = ["order_id", "order_date", "order_daily_sales_amount"]
    assert result.columns == expected_columns

def test_normalize_column_with_mixed_special_chars(spark, df_with_mixed_chars_columns):
    result = normalize_column_names(spark = spark, df = df_with_mixed_chars_columns)
    expected_columns = ["order_id", "order_date", "order_daily_sales_amount"]
    assert result.columns == expected_columns

def test_drop_columns(spark, df_dropped_columns):
    original_len = len(df_dropped_columns.columns)
    result = drop_column(spark=spark, df=df_dropped_columns, column="col_a")
    actual_len = len(result.columns)
    expected_columns = ["col_b"]
    assert result.columns == expected_columns
    assert actual_len == original_len - 1

def test_drop_empty_column(spark, df_null_column):
    original_rows = df_null_column.count()
    result = drop_column(spark=spark, df=df_null_column, column="name")
    rows = result.count()
    assert original_rows == rows

def test_drop_single_column_df(spark, df_single_column):
    result = drop_column(spark=spark, df=df_single_column, column="col_a")
    expected_columns = []
    assert result.columns == expected_columns

def test_basic_round_column(spark, df_basic_round_value):
    result = round_value(
        spark=spark,
        df=df_basic_round_value,
        column="amount",
        round_to=2
    )
    actual = [row.amount for row in result.orderBy("id").collect()]
    assert actual == [12.35, 67.89]

def test_basic_round_less_precision_column(spark, df_basic_round_value):
    result = round_value(
        spark=spark,
        df=df_basic_round_value,
        column="amount",
        round_to=1
    )
    actual = [row.amount for row in result.orderBy("id").collect()]
    assert actual == [12.3, 67.9]

def test_basic_round_negative_column(spark, df_basic_round_negative_value):
    result = round_value(
        spark=spark,
        df=df_basic_round_negative_value,
        column="amount",
        round_to=2
    )
    actual = [row.amount for row in result.orderBy("id").collect()]
    assert actual == [-12.35, -67.89]

def test_basic_round_null_column(spark, df_basic_round_null_value):
    result = round_value(
        spark=spark,
        df=df_basic_round_null_value,
        column="amount",
        round_to=2
    )
    actual = [row.amount for row in result.orderBy("id").collect()]
    assert actual == [7.39, None]

def test_basic_round_int_column(spark, df_basic_round_int_value):
    result = round_value(
        spark=spark,
        df=df_basic_round_int_value,
        column="amount",
        round_to=2
    )
    actual = [row.amount for row in result.orderBy("id").collect()]
    assert actual == [7, 3]

def test_basic_round_column_no_df_sideeffect(spark, df_basic_round_value):
    result = round_value(
        spark=spark,
        df=df_basic_round_value,
        column="amount",
        round_to=2
    )
    row = result.collect()[0]
    assert row.id == 1


def test_to_date_col_default_format(spark, df_date_col_default_format):
    result = to_date_col(spark, df_date_col_default_format, "dt")
    rows = result.collect()
    assert str(rows[0].dt) == "2026-01-01"
    assert str(rows[1].dt) == "2026-01-02"
    assert isinstance(result.schema["dt"].dataType, DateType)
    assert result.columns == ["dt"]

def test_to_date_col_custom_format(spark, df_date_col_custom_format):
    result = to_date_col(spark, df_date_col_custom_format, "dt", fmt="yyyy-MM-dd")
    rows = result.collect()
    assert str(rows[0].dt) == "2026-01-01"
    assert str(rows[1].dt) == "2026-01-02"
    assert isinstance(result.schema["dt"].dataType, DateType)
    assert result.columns == ["dt"]

def test_to_date_col_invalid_dates(spark, df_date_col_invalid_dates):
    result = to_date_col(spark, df_date_col_invalid_dates, "dt")
    data = [(None,), (None,)]
    df = spark.createDataFrame(data, "dt DATE")
    assertDataFrameEqual(result, df)
    
def test_to_datetime_col_default_format(spark, df_date_col_default_format):
    result = to_timestamp_col(spark, df_date_col_default_format, "dt")
    rows = result.collect()
    assert str(rows[0].dt) == "2026-01-01 00:00:00"
    assert str(rows[1].dt) == "2026-01-02 00:00:00"
    assert isinstance(result.schema["dt"].dataType, TimestampType)
    assert result.columns == ["dt"]

def test_to_datetime_col_custom_format(spark, df_date_col_custom_format):
    result = to_timestamp_col(spark, df_date_col_custom_format, "dt", fmt="yyyy-MM-dd")
    rows = result.collect()
    assert str(rows[0].dt) == "2026-01-01 00:00:00"
    assert str(rows[1].dt) == "2026-01-02 00:00:00"
    assert isinstance(result.schema["dt"].dataType, TimestampType)
    assert result.columns == ["dt"]


@pytest.fixture
def df_nulls_string_column(spark):
    df = spark.createDataFrame(
        [("a",), (None,), ("c",)],
        ["col"]
    )
    return df

@pytest.fixture
def df_nulls_int_column(spark):
    df = spark.createDataFrame(
        [(10,), (None,)],
        ["col"]
    )
    return df

@pytest.fixture
def df_nulls_null_column(spark):
    df = spark.createDataFrame(
        [(None,), (None,)],
        "col STRING"
    )
    return df

@pytest.fixture
def df_nulls_null_column_no_side_effect(spark):
    df = spark.createDataFrame(
        [(1, None,), (2, None,)],
        "id INT, col STRING"
    )
    return df

@pytest.fixture
def df_names_variations(spark):
    data = [
        (1, "       Kristi;'[]na Nunn"),
        (2, "             Dorothy Wardle"),
        (3, "         =--Katharine Harms"),
        (4, "     Rac5467hel Payne"),
        (5, "_12312Patrick Bzostek"),
        (6, ")(*&Sung Pak"),
        (7, "   _Mike Vitt 12313orini")
    ]
    df = spark.createDataFrame(data, ["id", "name"])
    return df



def test_fill_nulls_string_column(spark, df_nulls_string_column):
    result = fill_nulls(spark, df_nulls_string_column, "col", "unknown")
    values = [r.col for r in result.collect()]
    assert values == ["a", "unknown", "c"]
    assert result.columns == ["col"]

def test_fill_nulls_int_column(spark, df_nulls_int_column):
    result = fill_nulls(spark, df_nulls_int_column, "col", 0)
    values = [r.col for r in result.collect()]
    assert values == [10,0]
    assert result.columns == ["col"]

def test_fill_nulls_null_column(spark, df_nulls_null_column):
    result = fill_nulls(spark, df_nulls_null_column, "col", "unknown")
    values = [r.col for r in result.collect()]
    assert values == ["unknown","unknown"]
    assert result.columns == ["col"]

def test_fill_nulls_null_column(spark, df_nulls_null_column_no_side_effect):
    result = fill_nulls(spark, df_nulls_null_column_no_side_effect, "col", "unknown")
    row = result.collect()[0]
    assert row.id == 1
    assert row.col == "unknown"


def test_name_transformation(spark, df_names_variations):
    df = transform_names_column(spark, df_names_variations, "name")
    result = [row.name for row in df.select("name").collect()]
    print(result)
    assert result == ['Kristina Nunn', 'Dorothy Wardle', 'Katharine Harms', 'Rachel Payne', 'Patrick Bzostek', 'Sung Pak', 'Mike Vitt orini']
    