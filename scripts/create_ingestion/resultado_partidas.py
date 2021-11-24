from pyspark.sql.types import IntegerType, StructType, StructField, StringType
from pyspark.sql import DataFrame
from default import spark, sqlContext

class ResultadoPartidas():

    def create_schema(self) -> StructType:
        resultado_partidas_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("rodada_id", IntegerType(), True),
                StructField("time_casa_id", IntegerType(), True),
                StructField("time_visitante_id", IntegerType(), True),
                StructField("placar_time_casa", IntegerType(), True),
                StructField("placar_time_visitante", IntegerType(), True),
                StructField("resultado",StringType(),True),
            ]
        )

        return resultado_partidas_schema

    def load_dataframe(self, year, resultado_partidas_schema):
        df_resultado_partidas = spark.read.format("csv") \
            .option("header", True) \
            .schema(resultado_partidas_schema) \
            .load("/raw_data/" + year + "/" + year + "_partidas_ids.csv")

        df_resultado_partidas.createOrReplaceTempView("temp_resultado_partidas_" + year)

    def create_table(self):
        sqlContext.sql("CREATE TABLE resultado_partidas_" + year + " AS SELECT * FROM temp_resultado_partidas_" + year)


if __name__ == "__main__":
    for year in ["2014", "2015", "2016"]:
        resultado_partidas_schema = ResultadoPartidas().create_schema()
        ResultadoPartidas().load_dataframe(year, resultado_partidas_schema)
        ResultadoPartidas().create_table()