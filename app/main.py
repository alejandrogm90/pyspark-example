from pyspark.sql import SparkSession

# Inicializar una Spark Session
spark = SparkSession.builder \
    .appName("Mi Proyecto PySpark") \
    .getOrCreate()

# Crear un DataFrame de ejemplo
data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
columns = ["Name", "Value"]

df = spark.createDataFrame(data, columns)

# Mostrar el contenido del DataFrame
df.show()

# Detener la Spark Session
spark.stop()
