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

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

spark.sql("DROP DATABASE IF EXISTS ingestion CASCADE")
spark.sql("DROP DATABASE IF EXISTS consumption CASCADE")
