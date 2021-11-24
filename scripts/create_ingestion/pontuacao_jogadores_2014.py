from pyspark.sql.types import IntegerType, StructType, StructField, StringType, FloatType
from pyspark.sql import DataFrame
from default import spark, sqlContext

class PontuacaoJogadores():

    def create_schema(self) -> StructType:
        pontuacao_jogadores_schema = StructType(
            [
                StructField("atleta_id", IntegerType(), True),
                StructField("rodada_id", IntegerType(), True),
                StructField("clube_id", IntegerType(), True),
                StructField("participou", IntegerType(), True),
                StructField("posicao_id", IntegerType(), True),
                StructField("jogos", IntegerType(), True),
                StructField("pontos", FloatType(), True),
                StructField("pontos_media", FloatType(), True),
                StructField("preco", FloatType(), True),
                StructField("preco_variacao", FloatType(), True),
                StructField("partida_id", IntegerType(), True),
                StructField("mando", IntegerType(), True),
                StructField("titular", IntegerType(), True),
                StructField("substituido", IntegerType(), True),
                StructField("tempo_jogado", FloatType(), True),
                StructField("nota", FloatType(), True),
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

    def load_dataframe(self, pj_schema):
        df_pontuacao_jogadores = spark.read.format("csv") \
            .option("header", True) \
            .schema(pj_schema) \
            .load("/raw_data/2014/2014_scouts_raw.csv")

        df_pontuacao_jogadores.createOrReplaceTempView("temp_pontuacao_jogadores_2014")

    def create_table(self):
        sqlContext.sql("CREATE TABLE pontuacao_jogadores_2014 AS SELECT * FROM temp_pontuacao_jogadores_2014")


if __name__ == "__main__":
    pj_schema = PontuacaoJogadores().create_schema()
    PontuacaoJogadores().load_dataframe(pj_schema)
    PontuacaoJogadores().create_table()