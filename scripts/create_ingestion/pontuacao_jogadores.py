from pyspark.sql.types import IntegerType, StructType, StructField, StringType, FloatType
from pyspark.sql import DataFrame
from default import spark, sqlContext

class PontuacaoJogadores():

    def create_schema(self) -> StructType:
        pontuacao_jogadores_schema = StructType(
            [
                StructField("rodada_id", IntegerType(), True),
                StructField("clube_id", IntegerType(), True),
                StructField("atleta_id", IntegerType(), True),                
                StructField("participou", StringType(), True),
                StructField("pontos", FloatType(), True),
                StructField("pontos_media", FloatType(), True),
                StructField("preco", FloatType(), True),
                StructField("preco_variacao", FloatType(), True),
                StructField("FS", IntegerType(), True),
                StructField("PE", IntegerType(), True),
                StructField("A", IntegerType(), True),
                StructField("FT", IntegerType(), True),
                StructField("FD", IntegerType(), True),
                StructField("FF", IntegerType(), True),
                StructField("G", IntegerType(), True),
                StructField("I", IntegerType(), True),
                StructField("PP", IntegerType(), True),
                StructField("RB", IntegerType(), True),
                StructField("FC", IntegerType(), True),
                StructField("GC", IntegerType(), True),
                StructField("CA", IntegerType(), True),
                StructField("CV", IntegerType(), True),
                StructField("SG", IntegerType(), True),
                StructField("DD", IntegerType(), True),
                StructField("DP", IntegerType(), True),
                StructField("GS", IntegerType(), True),
            ]
        )

        return pontuacao_jogadores_schema

    def load_dataframe(self, year, pj_schema):
        df_pontuacao_jogadores = spark.read.format("csv") \
            .option("header", True) \
            .schema(pj_schema) \
            .load("/raw_data/" + year + "/" + year + "_scouts_raw.csv")

        df_pontuacao_jogadores.createOrReplaceTempView("temp_pontuacao_jogadores_" + year)

    def create_table(self):
        sqlContext.sql("CREATE TABLE pontuacao_jogadores_" + year + " AS SELECT * FROM temp_pontuacao_jogadores_" + year)


if __name__ == "__main__":
    for year in ["2015", "2016"]:
        pj_schema = PontuacaoJogadores().create_schema()
        PontuacaoJogadores().load_dataframe(year, pj_schema)
        PontuacaoJogadores().create_table()