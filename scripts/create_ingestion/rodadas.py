from pyspark.sql.types import IntegerType, StructType, StructField, StringType, FloatType
from pyspark.sql import DataFrame
from default import spark, sqlContext

class Rodadas():

    def create_schema(self) -> StructType:
        rodadas_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("nome", StringType(), True),
                StructField("atleta_slug", StringType(), True),
                StructField("atleta_apelido", StringType(), True),
                StructField("atleta_foto", StringType(), True),
                StructField("atleta_id", StringType(), True),
                StructField("rodada_id", StringType(), True),
                StructField("clube_id", StringType(), True),
                StructField("posicao_id", StringType(), True),
                StructField("status_id", StringType(), True),
                StructField("pontos_num", StringType(), True),
                StructField("preco_num", StringType(), True),
                StructField("variacao_num", StringType(), True),
                StructField("media_num", StringType(), True),
                StructField("clube_name", StringType(), True),
                StructField("FC", StringType(), True),
                StructField("FD", StringType(), True),
                StructField("FF", StringType(), True),
                StructField("FS", StringType(), True),
                StructField("G", StringType(), True),
                StructField("I", StringType(), True),
                StructField("RB", StringType(), True),
                StructField("CA", StringType(), True),
                StructField("PE", StringType(), True),
                StructField("A", StringType(), True),
                StructField("SG", StringType(), True),
                StructField("DD", StringType(), True),
                StructField("FT", StringType(), True),
                StructField("GS", StringType(), True),
                StructField("CV", StringType(), True),
                StructField("GC", StringType(), True),
            ]
        )

        return rodadas_schema

    def load_dataframe(self, year, rodadas_schema):
        for rodada in range(1, 39):
            df_rodada = spark.read.format("csv") \
                .option("header", True) \
                .schema(rodadas_schema) \
                .load("/raw_data/" + year + "/rodada-" + str(rodada) + ".csv")

            df_rodada.write \
                .option("header", "true") \
                .mode("append") \
                .format("hive") \
                .saveAsTable("ingestion.rodadas_" + year)

    def create_table(self, year):
        sqlContext.sql("CREATE TABLE IF NOT EXISTS rodadas_" + year + " (\
            id INT,\
            nome STRING,\
            atleta_slug STRING,\
            atleta_apelido STRING,\
            atleta_foto STRING,\
            atleta_id INT,\
            rodada_id INT,\
            clube_id STRING,\
            posicao_id STRING,\
            status_id STRING,\
            pontos_num FLOAT,\
            preco_num FLOAT,\
            variacao_num FLOAT,\
            media_num FLOAT,\
            clube_name STRING,\
            FC FLOAT,\
            FD FLOAT,\
            FF FLOAT,\
            FS FLOAT,\
            G FLOAT,\
            I FLOAT,\
            RB FLOAT,\
            CA FLOAT,\
            PE FLOAT,\
            A FLOAT,\
            SG FLOAT,\
            DD FLOAT,\
            FT FLOAT,\
            GS FLOAT,\
            CV FLOAT,\
            GC FLOAT)"
        )


if __name__ == "__main__":
    for year in ["2018", "2019", "2020"]:
        rodadas_schema = Rodadas().create_schema()
        Rodadas().create_table(year)
        Rodadas().load_dataframe(year, rodadas_schema)