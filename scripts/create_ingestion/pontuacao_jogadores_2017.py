from pyspark.sql.types import IntegerType, StructType, StructField, StringType, FloatType
from pyspark.sql import DataFrame
from default import spark, sqlContext

class PontuacaoJogadores():

    def create_schema(self) -> StructType:
        pontuacao_jogadores_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("A", IntegerType(), True),
                StructField("CA", IntegerType(), True),
                StructField("CV", IntegerType(), True),
                StructField("DD", IntegerType(), True),
                StructField("DP", IntegerType(), True),
                StructField("FC", IntegerType(), True),
                StructField("FD", IntegerType(), True),
                StructField("FF", IntegerType(), True),
                StructField("FS", IntegerType(), True),
                StructField("FT", IntegerType(), True),
                StructField("G", IntegerType(), True),
                StructField("GC", IntegerType(), True),
                StructField("GS", IntegerType(), True),
                StructField("I", IntegerType(), True),
                StructField("PE", IntegerType(), True),
                StructField("PP", IntegerType(), True),
                StructField("RB", IntegerType(), True),
                StructField("SG", IntegerType(), True),
                StructField("nota", FloatType(), True),
                StructField("atleta_apelido", StringType(), True),
                StructField("atleta_id", IntegerType(), True),
                StructField("clube_nome", StringType(), True),
                StructField("clube_slug", StringType(), True),
                StructField("atleta_foto", StringType(), True),
                StructField("atleta_jogos_num", IntegerType(), True),
                StructField("atleta_media_num", FloatType(), True),
                StructField("atleta_nome", StringType(), True),
                StructField("pontos", FloatType(), True),
                StructField("atleta_posicao_abreviacao", StringType(), True),
                StructField("preco", FloatType(), True),
                StructField("rodada_id", IntegerType(), True),
                StructField("status", StringType(), True),
                StructField("preco_variacao", FloatType(), True),                
            ]
        )

        return pontuacao_jogadores_schema

    def load_dataframe(self, pj_schema):
        df_pontuacao_jogadores = spark.read.format("csv") \
            .option("header", True) \
            .schema(pj_schema) \
            .load("/raw_data/2017/2017_scouts_raw.csv")

        df_pontuacao_jogadores.createOrReplaceTempView("temp_pontuacao_jogadores_2017")

    def create_table(self):
        sqlContext.sql("CREATE TABLE pontuacao_jogadores_2017 AS SELECT * FROM temp_pontuacao_jogadores_2017")


if __name__ == "__main__":
    pj_schema = PontuacaoJogadores().create_schema()
    PontuacaoJogadores().load_dataframe(pj_schema)
    PontuacaoJogadores().create_table()