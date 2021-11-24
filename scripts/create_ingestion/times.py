from pyspark.sql.types import IntegerType, StructType, StructField, StringType
from pyspark.sql import DataFrame
from default import spark, sqlContext

class Times():
    def __init__(self) -> None:
        super().__init__()

    def create_schema(self) -> StructType:
        times_schema = StructType(
            [
                StructField("nome_cpf",StringType(),True),
                StructField("nome_cartola",StringType(),True),
                StructField("nome_completo",StringType(),True),
                StructField("codigo_antigo", IntegerType(), True),
                StructField("codigo_2017", IntegerType(), True),
                StructField("codigo_2018", IntegerType(), True),
                StructField("id", IntegerType(), True),
                StructField("abreviacao",StringType(),True),
                StructField("escudo_60",StringType(),True),
                StructField("escudo_45",StringType(),True),
                StructField("escudo_30",StringType(),True),
            ]
        )

        return times_schema

    def load_dataframe(self, times_schema):
        df_times = spark.read.format("csv") \
            .option("header", True) \
            .schema(times_schema) \
            .load("/raw_data/times_ids.csv")

        df_times.createOrReplaceTempView("temp_times")

    def create_table(self):
        sqlContext.sql("CREATE TABLE times AS SELECT * FROM temp_times")


if __name__ == "__main__":
    times_schema = Times().create_schema()
    Times().load_dataframe(times_schema)
    Times().create_table()