from pyspark.sql import SparkSession, DataFrame
from typing import List

spark = (
    SparkSession.builder.config("spark.sql.crossJoin.enabled", "true")
    .config(
        "hive.metastore.client.factory.class"
    )
    .config(
        "spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse"
    )
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql("CREATE DATABASE IF NOT EXISTS consumption")
spark.sql("use consumption")

def create_table(table_name, fields):
    spark.sql("CREATE TABLE IF NOT EXISTS consumption." + \
            table_name + " (" + fields + ")"
    )