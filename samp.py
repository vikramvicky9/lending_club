from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Simple PySpark Example with DataFrame") \
    .getOrCreate()

try:
    # Define file paths
    input_path = "C:/Users/Vicky/Desktop/lending_club/data/customers.csv"
    output_path = "C:/Users/Vicky/Desktop/lending_club/data/customers1"

    # Read the CSV file
    df = spark.read.csv(input_path, inferSchema=True, header=True)

    # Drop duplicates
    df1 = df.dropDuplicates()

    # Ensure the output directory exists
    output_dir = os.path.dirname(output_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Write the DataFrame to a CSV file
    df1.write.csv(output_path, mode="overwrite")

except Exception as e:
    print("An error occurred:", e)
    # Print the full stack trace
    import traceback
    traceback.print_exc()

finally:
    # Stop the Spark session
    spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Simple PySpark Example with DataFrame") \
    .getOrCreate()

try:
    # Read the CSV file
    df = spark.read.csv("C:/Users/Vicky/Desktop/lending_club/data/customers.csv", inferSchema=True, header=True)

    # Drop duplicates
    df1 = df.dropDuplicates()
    

    # Write the DataFrame to a CSV file
    df1.write.csv("C:/Users/Vicky/Desktop/lending_club/data/customers1.csv", mode="overwrite")

except Exception as e:
    print("An error occurred:", e)

finally:
    # Stop the Spark session
    spark.stop()