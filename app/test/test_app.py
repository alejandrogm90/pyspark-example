import unittest
from app_2 import create_dataframe_from_csv, get_average_population, get_cities_with_capital

class TestCityData(unittest.TestCase):

    def setUp(self):
        self.df = create_dataframe_from_csv('data/cities.csv')

    def test_create_dataframe_from_csv(self):
        self.assertEqual(self.df.count(), 10)  # Debe haber 10 ciudades
        self.assertListEqual(self.df.columns, ["nombre", "poblacion", "tamano", "pais", "es_capital"])

    def test_get_average_population(self):
        average_population = get_average_population(self.df)
        self.assertAlmostEqual(average_population, 14790153.4, delta=0.01)  # Promedio según los datos

    def test_get_cities_with_capital(self):
        capitals = get_cities_with_capital(self.df)
        self.assertEqual(capitals.count(), 3)  # Según los datos, debe haber 3 ciudades capitales
        self.assertListEqual(
            [row['nombre'] for row in capitals.select("nombre").collect()],
            ["Tokio", "Ciudad de México", "Nueva York"]
        )


if __name__ == '__main__':
    unittest.main()
