from typing import List, Tuple
from pyspark.sql import SparkSession
from delta.tables import DeltaTable, DataFrame

def normalize_column_names(
    spark: SparkSession,
    df: DataFrame
) -> DataFrame:
    for col in df.columns:
        df = df.withColumnRenamed(col, col.replace(" ", "_").lower())
    return df
    