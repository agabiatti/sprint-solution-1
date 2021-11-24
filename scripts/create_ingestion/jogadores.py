from pyspark.sql.types import IntegerType, StructType, StructField, StringType
from pyspark.sql import DataFrame
from default import spark, sqlContext

class Jogadores():
    def create_schema(self) -> StructType:
        jogadores_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("nome",StringType(),True),
                StructField("clube_id",IntegerType(),True),
                StructField("posicao_id",IntegerType(),True),
            ]
        )

        return jogadores_schema

    def load_dataframe(self, year, jogadores_schema):
        df_jogadores = spark.read.format("csv") \
            .option("header", True) \
            .schema(jogadores_schema) \
            .load("/raw_data/" + year + "/" + year + "_jogadores.csv")

        df_jogadores.createOrReplaceTempView("temp_jogadores_" + year)

    def create_table(self, year):
        sqlContext.sql("CREATE TABLE jogadores_" + year + " AS SELECT * FROM temp_jogadores_" + year)

if __name__ == "__main__":
    for year in ["2014", "2015", "2016", "2017"]:
        jogadores_schema = Jogadores().create_schema()
        Jogadores().load_dataframe(year, jogadores_schema)
        Jogadores().create_table(year)