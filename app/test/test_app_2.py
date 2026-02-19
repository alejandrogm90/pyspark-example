"""
test_app_2.py

Basic example of test
"""
import unittest
from pyspark.sql import SparkSession
from src.app_2 import create_dataframe_from_csv, get_average_population, get_cities_with_capital, \
    get_cities_with_population_range


class TestApp2(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestApp2") \
            .master("local[*]") \
            .getOrCreate()

        # Crear un DataFrame de ejemplo para las pruebas
        data = [
            ("Tokio", 13929286, 2194, "Japón", 1),
            ("Délhi", 30290936, 1483, "India", 0),
            ("Shanghái", 26317104, 6340, "China", 0),
            ("Ciudad de México", 9209944, 1485, "México", 1)
        ]

        columns = ["nombre", "poblacion", "tamano", "pais", "es_capital"]
        cls.df = cls.spark.createDataFrame(data, columns)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_get_average_population(self):
        average = get_average_population(self.df)
        self.assertAlmostEqual(average, 16456840)

    def test_get_cities_with_capital(self):
        capitals_df = get_cities_with_capital(self.df)
        self.assertEqual(capitals_df.count(), 2)  # Hay 2 capitales en el conjunto de datos.

    def test_get_cities_with_population_range(self):
        filtered_df = get_cities_with_population_range(self.df, 10000000, 30000000)
        self.assertEqual(filtered_df.count(), 3)  # Debe haber 3 ciudades en el rango.
        self.assertTrue(
            all(row['poblacion'] >= 10000000 and row['poblacion'] <= 30000000 for row in filtered_df.collect()))


if __name__ == '__main__':
    unittest.main()

