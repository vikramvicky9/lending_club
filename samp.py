from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Simple PySpark Example with DataFrame") \
    .getOrCreate()

df=spark.read.csv("C://Users//Vicky//Desktop//lending_club//data//customers.csv",inferSchema=True,header=True)


df.show()
print("schema")
df.printSchema()
spark.stop()