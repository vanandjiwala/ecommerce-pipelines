from typing import List, Tuple
from pyspark.sql import SparkSession
from delta.tables import DeltaTable, DataFrame

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

def write_to_delta_with_cdc_by_name(
    spark: SparkSession,
    df: DataFrame,
    catalog: str,
    schema: str,
    table: str,
    merge_keys: list[str],
    append_only: bool = False
) -> None:  
    table_name = f"{catalog}.{schema}.{table}" 
    if spark.catalog.tableExists(table_name):
        if append_only:
            df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)
        else:
            delta_table = DeltaTable.forName(spark, table_name)
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
            
            delta_table.alias("target").merge(
                df.alias("source"),
                merge_condition
            ).withSchemaEvolution().whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").option("mergeSchema", "true").option("delta.enableChangeDataFeed", "true").saveAsTable(table_name)