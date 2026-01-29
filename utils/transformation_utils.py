from typing import List, Tuple, Any
from pyspark.sql import SparkSession
from delta.tables import DeltaTable, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

def normalize_column_names(
    spark: SparkSession,
    df: DataFrame
) -> DataFrame:
    for col in df.columns:
        df = df.withColumnRenamed(col, col.replace(" ", "_").lower())
    return df

def transform_names_column(
    spark: SparkSession,
    df: DataFrame,
    name_column: str
) -> DataFrame:
    df_clean = df.withColumn(
        name_column,
        F.regexp_replace(name_column, "[^A-Za-z,\\. ]", "")
    ).withColumn(
        name_column,
        F.regexp_replace(name_column, "[0-9]", "")
    ).withColumn(
        name_column,
        F.regexp_replace(name_column, "\\.", " ")
    ).withColumn(
        name_column,
        F.regexp_replace(name_column, "\\s*,\\s*", ",")
    ).withColumn(
        name_column,
        F.regexp_replace(name_column, "\\s+", " ")
    ).withColumn(
        name_column,
        F.coalesce(name_column, F.lit("Not Available"))
    ).withColumn(
        name_column,
        F.trim(name_column)
    )
    return df_clean

def drop_column(
    spark: SparkSession,
    df: DataFrame,
    column: str
) -> DataFrame:
    df = df.drop(column)
    return df


def deduplicate_data_by_time(
    spark: SparkSession,
    df: DataFrame, 
    partition_cols: List[str], 
    order_col: str
) -> DataFrame:
    window_spec = Window.partitionBy(*partition_cols).orderBy(F.col(order_col).desc())
    df = df.withColumn("row_number", F.row_number().over(window_spec))
    df = df.filter(F.col("row_number") == 1).drop("row_number")
    return df

def round_value(
    spark: SparkSession,
    df: DataFrame,
    column: str,
    round_to: int
) -> DataFrame:
    df = df.withColumn(column, F.round(F.col(column), round_to))
    return df

def to_date_col(spark: SparkSession, df: DataFrame, col_name: str, fmt: str = "MM/dd/yyyy") -> DataFrame:
    df = df.withColumn(col_name, F.to_date(F.col(col_name), fmt))
    return df

def to_timestamp_col(spark: SparkSession, df: DataFrame, col_name: str, fmt: str = "MM/dd/yyyy") -> DataFrame:
    df = df.withColumn(col_name, F.to_timestamp(F.col(col_name), fmt))
    return df

def fill_nulls(spark: SparkSession, df: DataFrame, col_name: str, value: Any) -> DataFrame:
    df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(value)))
    return df