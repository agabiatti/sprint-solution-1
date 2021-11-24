from pyspark.sql import SparkSession, HiveContext

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

sc = spark.sparkContext
sqlContext = HiveContext(sc)

sqlContext.sql("CREATE DATABASE IF NOT EXISTS ingestion")
sqlContext.sql("use ingestion")