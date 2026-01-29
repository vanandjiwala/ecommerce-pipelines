from typing import List, Tuple, Any
from pyspark.sql import SparkSession
from delta.tables import DeltaTable, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

def normalize_column_names(
    spark: SparkSession,
    df: DataFrame
) -> DataFrame:
    """
    normalize column names. 
    Function uses lower casing and replaces spaces with underscore.

    Args:
        spark (spark): spark session
        df (DataFrame): dataframe

    Returns:
        df: df with updated column names
    """
    for col in df.columns:
        df = df.withColumnRenamed(col, col.replace(" ", "_").lower())
    return df

def transform_names_column(
    spark: SparkSession,
    df: DataFrame,
    name_column: str
) -> DataFrame:
    """
    This function will handle the name column in customer dataset
    Args:
        spark (spark): spark session
        df (DataFrame): dataframe
        name_column: name column title
    Returns:
        df: df with updated names
    """
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
    """
    This function will drop a column from df
    Args:
        spark (spark): spark session
        df (DataFrame): dataframe
        column: name of the column
    Returns:
        df: df with dropped columns
    """
    df = df.drop(column)
    return df


def deduplicate_data_by_time(
    spark: SparkSession,
    df: DataFrame, 
    partition_cols: List[str], 
    order_col: str
) -> DataFrame:
    """
    This function will drop duplicated based on latest ingest datetime
    We are using row_number window function to ensure only 1 record is valid
    Args:
        spark (spark): spark session
        df (DataFrame): dataframe
        partition_cols: list of columns used in the window partition clause
        order_col: ordering column - typically it is ingested_timestamp
    Returns:
        df: df with dropped duplicates
    """
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
    """
    Round a column in a dataframe
    Args:
        spark (spark): spark session
        df (DataFrame): dataframe
        column: name of the column
        round_to: precision points
    Returns:
        df: df with dropped duplicates
    """
    df = df.withColumn(column, F.round(F.col(column), round_to))
    return df

def to_date_col(spark: SparkSession, df: DataFrame, col_name: str, fmt: str = "MM/dd/yyyy") -> DataFrame:
    """
    Date conversion
    Args:
        spark (spark): spark session
        df (DataFrame): dataframe
        col_name: name of the column
        fmt: date format
    Returns:
        df: df with conversion
    """
    df = df.withColumn(col_name, F.to_date(F.col(col_name), fmt))
    return df

def to_timestamp_col(spark: SparkSession, df: DataFrame, col_name: str, fmt: str = "MM/dd/yyyy") -> DataFrame:
    """
    timestamp conversion
    Args:
        spark (spark): spark session
        df (DataFrame): dataframe
        col_name: name of the column
        fmt: date format
    Returns:
        df: df with conversion
    """
    df = df.withColumn(col_name, F.to_timestamp(F.col(col_name), fmt))
    return df

def fill_nulls(spark: SparkSession, df: DataFrame, col_name: str, value: Any) -> DataFrame:
        """
    Date conversion
    Args:
        spark (spark): spark session
        df (DataFrame): dataframe
        col_name: name of the column
        value: default value
    Returns:
        df: df with null column filled with default value
    """
    df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(value)))
    return df