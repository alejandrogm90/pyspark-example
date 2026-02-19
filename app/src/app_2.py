import os
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession


def create_dataframe_from_csv(sesion, file_path) -> Optional[DataFrame]:
    try:
        return sesion.read.csv(file_path, header=True, inferSchema=True)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return None


def get_average_population(df) -> DataFrame:
    return df.agg({"poblacion": "avg"}).first()[0]


def get_cities_with_capital(df) -> DataFrame:
    #return df.filter(df.es_capital.cast("boolean") == True)
    return df.filter(df.es_capital == 1)  # Verifica que sea igual a 1 en lugar de True


if __name__ == '__main__':
    if sys.argv.__len__() < 2:
        print("File path must be given as argument.")
        exit(1)
    if not os.path.exists(sys.argv[1]):
        print(f"File not found in: {sys.argv[1]}")
        exit(2)

    spark = (SparkSession.builder.
             appName("CitiesExample").
             master("local[*]").
             enableHiveSupport().
             getOrCreate())

    print("--> ALL CITIES")
    tmp_df = create_dataframe_from_csv(spark, sys.argv[1])
    tmp_df.printSchema()  # Esto mostrará el esquema del DataFrame
    tmp_df.show()
    print("--> ALL CITIES ORDERED BY (nombre)")
    tmp_df = tmp_df.orderBy("nombre")
    tmp_df.show()
    print("--> ONLY CAPITALS")
    print(tmp_df)
    tmp_df = get_cities_with_capital(tmp_df)
    tmp_df.show()
    print("--> AVERAGE POPULATION")
    average_population = get_average_population(tmp_df)
    print(f"average_population = {average_population}")

    spark.stop()
