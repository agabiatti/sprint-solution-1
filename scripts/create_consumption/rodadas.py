from pyspark.sql.types import IntegerType, StructType, StructField, StringType
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, udf, when
from default import spark, create_table

fields = "id INTEGER," + \
    " nome STRING," + \
    " posicao_id INTEGER," + \
    " rodada_id INTEGER," + \
    " pontos FLOAT," + \
    " ano INTEGER"

table_name = "consumption.rodadas"

create_table("rodadas", fields)

df_rodadas_2014 = spark.sql("select j.id, j.nome, j.posicao_id, p.rodada_id, p.pontos, 2014 as ano from" + \
                            " ingestion.jogadores_2014 j" + \
                            " inner join ingestion.pontuacao_jogadores_2014 p on p.atleta_id = j.id")

df_rodadas_2015 = spark.sql("select j.id, j.nome, j.posicao_id, p.rodada_id, p.pontos, 2015 as ano from" + \
                            " ingestion.jogadores_2015 j" + \
                            " inner join ingestion.pontuacao_jogadores_2015 p on p.atleta_id = j.id")

df_rodadas_2016 = spark.sql("select j.id, j.nome, j.posicao_id, p.rodada_id, p.pontos, 2016 as ano from" + \
                            " ingestion.jogadores_2016 j" + \
                            " inner join ingestion.pontuacao_jogadores_2016 p on p.atleta_id = j.id")

df_rodadas_2017 = spark.sql("select j.id, j.nome, j.posicao_id, p.rodada_id, p.pontos, 2017 as ano from" + \
                            " ingestion.jogadores_2017 j" + \
                            " inner join ingestion.pontuacao_jogadores_2017 p on p.atleta_id = j.id")

df_rodadas_2018 = spark.sql("select atleta_id as id, nome, posicao_id, rodada_id, pontos_num as pontos, 2018 as ano from ingestion.rodadas_2018")
df_rodadas_2019 = spark.sql("select atleta_id as id, nome, posicao_id, rodada_id, pontos_num as pontos, 2019 as ano from ingestion.rodadas_2019")
df_rodadas_2020 = spark.sql("select atleta_id as id, nome, posicao_id, rodada_id, pontos_num as pontos, 2020 as ano from ingestion.rodadas_2020")
df_posicoes = spark.sql("select * from ingestion.posicoes")

df_rodadas_2018_final = \
    df_rodadas_2018.alias("r18") \
    .join(
        df_posicoes.alias("p"),
        col("r18.posicao_id") == col("p.abreviacao")
    ) \
    .select(
        col("r18.id").alias("id"),
        col("r18.nome").alias("nome"),
        col("p.codigo").cast('int').alias("posicao_id"),
        col("r18.rodada_id").alias("rodada_id"),
        col("r18.pontos").alias("pontos"),
        col("r18.ano").alias("ano")
    )

df_rodadas_2019_final = \
    df_rodadas_2019.alias("r19") \
    .join(
        df_posicoes.alias("p"),
        col("r19.posicao_id") == col("p.abreviacao")
    ) \
    .select(
        col("r19.id").alias("id"),
        col("r19.nome").alias("nome"),
        col("p.codigo").cast('int').alias("posicao_id"),
        col("r19.rodada_id").alias("rodada_id"),
        col("r19.pontos").alias("pontos"),
        col("r19.ano").alias("ano")
    )

df_rodadas_2020_final = \
    df_rodadas_2020.alias("r20") \
    .join(
        df_posicoes.alias("p"),
        col("r20.posicao_id") == col("p.abreviacao")
    ) \
    .select(
        col("r20.id").alias("id"),
        col("r20.nome").alias("nome"),
        col("p.codigo").cast('int').alias("posicao_id"),
        col("r20.rodada_id").alias("rodada_id"),
        col("r20.pontos").alias("pontos"),
        col("r20.ano").alias("ano")
    )


df_rodadas_2014.write \
    .option("header", "true") \
    .mode("append") \
    .format("hive") \
    .saveAsTable(table_name)

df_rodadas_2015.write \
    .option("header", "true") \
    .mode("append") \
    .format("hive") \
    .saveAsTable(table_name)

df_rodadas_2016.write \
    .option("header", "true") \
    .mode("append") \
    .format("hive") \
    .saveAsTable(table_name)

df_rodadas_2017.write \
    .option("header", "true") \
    .mode("append") \
    .format("hive") \
    .saveAsTable(table_name)

df_rodadas_2018_final.write \
    .option("header", "true") \
    .mode("append") \
    .format("hive") \
    .saveAsTable(table_name)

df_rodadas_2019_final.write \
    .option("header", "true") \
    .mode("append") \
    .format("hive") \
    .saveAsTable(table_name)

df_rodadas_2020_final.write \
    .option("header", "true") \
    .mode("append") \
    .format("hive") \
    .saveAsTable(table_name)

