from typing import List, Tuple
from pyspark.sql import SparkSession

def print_name():
    return "Sample Project Function"

def create_catalog(spark: SparkSession, catalog: str) -> bool:
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        return True
    except Exception as e:
        print(e)
        return False

def create_schema(spark: SparkSession, catalog: str, schema: str) -> bool:
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        return True
    except Exception:
        return False


def create_table(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table_name: str,
    columns: List[Tuple[str, str]]
) -> bool:
    columns_sql = ", ".join([f"{col} {dtype}" for col, dtype in columns])
    sql = f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} (
        {columns_sql}
    )
    """
    try:
        spark.sql(sql)
        return True
    except Exception:
        return False

def table_exists(spark: SparkSession, catalog: str, schema: str, table_name: str) -> bool:
    return spark.catalog.tableExists("{catalog}.{schema}.{table_name}")


def create_volume(
    spark: SparkSession,
    catalog: str,
    schema: str, 
    volume: str
) -> bool:
    sql = f"""
    CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.`{volume}`
    """
    try:
        spark.sql(sql)
        return True
    except Exception:
        return False

def create_directory_path(
    spark: SparkSession,
    catalog: str, 
    schema: str, 
    volume: str, 
    directory: str) -> str:
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}/{directory}"
    return volume_path
