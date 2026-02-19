"""
app_2.py

Example using a csv input

Functions:
----------
- create_dataframe_from_csv(ss, file_path): returns a DataFrame
- get_average_population(df): returns a float average
- get_cities_with_capital(df): returns a DataFrame

"""
import os
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession


def create_dataframe_from_csv(ss: SparkSession, file_path: str) -> Optional[DataFrame]:
    """

    :param ss: SparkSession
    :param file_path: full file path
    :return: DataFrame from file input
    """
    try:
        return ss.read.csv(file_path, header=True, inferSchema=True)
    except IOError as e:
        print(f"Error reading CSV: {e}")
        return None


def get_average_population(df: DataFrame) -> DataFrame:
    """

    :param df: DataFrame input
    :return: average of colum 'poblacion'
    """
    return df.agg({"poblacion": "avg"}).first()[0]


def get_cities_with_capital(df: DataFrame) -> DataFrame:
    """

    :param df: DataFrame input
    :return:  DataFrame with only capitals
    """
    #return df.filter(df.es_capital.cast("boolean") == True)
    return df.filter(df.es_capital == 1)  # Verifica que sea igual a 1 en lugar de True


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("File path must be given as argument.")
        sys.exit(1)
    if not os.path.exists(sys.argv[1]):
        print(f"File not found in: {sys.argv[1]}")
        sys.exit(2)

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
