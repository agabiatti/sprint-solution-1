from pyspark.sql.types import IntegerType, StructType, StructField, StringType
from pyspark.sql import DataFrame
from default import spark, create_table

fields = "id INTEGER," + \
    " rodada_id INTEGER," + \
    " time_casa_id INTEGER," + \
    " time_visitante_id INTEGER," + \
    " placar_time_casa INTEGER," + \
    " placar_time_visitante INTEGER," + \
    " resultado STRING," + \
    " ano INTEGER"

create_table("resultado_partidas", fields)

df_resultado_partidas_2014 = spark.sql("select *, 2014 as ano from ingestion.resultado_partidas_2014")
df_resultado_partidas_2015 = spark.sql("select *, 2015 as ano from ingestion.resultado_partidas_2015")
df_resultado_partidas_2016 = spark.sql("select *, 2016 as ano from ingestion.resultado_partidas_2016")

df_resultado_partidas_2014.write \
    .option("header", "true") \
    .mode("append") \
    .format("hive") \
    .saveAsTable("consumption.resultado_partidas")

df_resultado_partidas_2015.write \
    .option("header", "true") \
    .mode("append") \
    .format("hive") \
    .saveAsTable("consumption.resultado_partidas")

df_resultado_partidas_2016.write \
    .option("header", "true") \
    .mode("append") \
    .format("hive") \
    .saveAsTable("consumption.resultado_partidas")