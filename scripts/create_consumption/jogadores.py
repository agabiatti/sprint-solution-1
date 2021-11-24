from pyspark.sql.types import IntegerType, StructType, StructField, StringType
from pyspark.sql import DataFrame
from default import spark, create_table

fields = "id INTEGER," + \
    " nome STRING," + \
    " clube_id INTEGER," + \
    " posicao_id INTEGER," + \
    " foto STRING," + \
    " ano INTEGER"

create_table("jogadores", fields)

df_jogadores_2014 = spark.sql("select *, '' as foto, 2014 as ano from ingestion.jogadores_2014")
df_jogadores_2015 = spark.sql("select *, '' as foto, 2015 as ano from ingestion.jogadores_2015")
df_jogadores_2016 = spark.sql("select *, '' as foto, 2016 as ano from ingestion.jogadores_2016")
df_jogadores_2017 = spark.sql("select *, '' as foto, 2017 as ano from ingestion.jogadores_2017")
df_jogadores_2018 = spark.sql("select j.id, j.nome, t.id clube_id, p.codigo posicao_id, j.foto" + \
                             ", 2018 as ano from ingestion.jogadores_2018 j" + \
                             " inner join ingestion.times t on t.nome_cartola = j.clube_id" + \
                             " inner join ingestion.posicoes p on p.posicao = j.posicao_id")

df_jogadores_2014.write \
    .option("header", "true") \
    .mode("append") \
    .format("hive") \
    .saveAsTable("consumption.jogadores")

df_jogadores_2015.write \
    .option("header", "true") \
    .mode("append") \
    .format("hive") \
    .saveAsTable("consumption.jogadores")

df_jogadores_2016.write \
    .option("header", "true") \
    .mode("append") \
    .format("hive") \
    .saveAsTable("consumption.jogadores")

df_jogadores_2017.write \
    .option("header", "true") \
    .mode("append") \
    .format("hive") \
    .saveAsTable("consumption.jogadores")

df_jogadores_2018.write \
    .option("header", "true") \
    .mode("append") \
    .format("hive") \
    .saveAsTable("consumption.jogadores")
