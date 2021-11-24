from pyspark.sql.types import IntegerType, StructType, StructField, StringType
from pyspark.sql import DataFrame
from default import spark, sqlContext

class Posicoes():
    def __init__(self) -> None:
        super().__init__()

    def create_schema(self) -> StructType:
        posicoes_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("codigo", StringType(), True),
                StructField("posicao",StringType(),True),
                StructField("abreviacao",StringType(),True),
            ]
        )

        return posicoes_schema

    def load_dataframe(self, posicoes_schema):
        df_posicoes = spark.read.format("csv") \
            .option("header", True) \
            .schema(posicoes_schema) \
            .load("/raw_data/posicoes_ids.csv")

        df_posicoes.createOrReplaceTempView("temp_posicoes")

    def create_table(self):
        sqlContext.sql("CREATE TABLE posicoes AS SELECT * FROM temp_posicoes")


if __name__ == "__main__":
    posicoes_schema = Posicoes().create_schema()
    Posicoes().load_dataframe(posicoes_schema)
    Posicoes().create_table()