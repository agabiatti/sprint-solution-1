from pyspark.sql.types import IntegerType, StructType, StructField, StringType
from pyspark.sql import DataFrame
from default import spark, sqlContext

class Jogadores():
    def create_schema(self) -> StructType:
        jogadores_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("nome", StringType(),True),
                StructField("clube_id", StringType(),True),
                StructField("posicao_id", StringType(),True),
                StructField("foto", StringType(),True),
            ]
        )

        return jogadores_schema

    def load_dataframe(self, jogadores_schema):
        df_jogadores = spark.read.format("csv") \
            .option("header", True) \
            .schema(jogadores_schema) \
            .load("/raw_data/2018/2018_jogadores.csv")

        df_jogadores.createOrReplaceTempView("temp_jogadores_2018")

    def create_table(self):
        sqlContext.sql("CREATE TABLE jogadores_2018 AS SELECT * FROM temp_jogadores_2018")

if __name__ == "__main__":
    jogadores_schema = Jogadores().create_schema()
    Jogadores().load_dataframe(jogadores_schema)
    Jogadores().create_table()