from pyspark.sql import SparkSession
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Simple PySpark Example with DataFrame") \
    .getOrCreate()

try:
    # Define file paths
    input_path = "C:/Users/Vicky/Desktop/lending_club/lending_club/data/customers.csv"
    output_path = "C:/Users/Vicky/Desktop/lending_club/customers1.csv"

    # Read the CSV file
    df = spark.read.csv(input_path, inferSchema=True, header=True)

    # Drop duplicates
    df1 = df.dropDuplicates()

    # Ensure DataFrame is not empty
    if df1.count() == 0:
        print("DataFrame is empty after dropping duplicates. No file will be written.")
    else:
        # Ensure the output directory exists or is empty
        if os.path.exists(output_path):
            import shutil
            shutil.rmtree(output_path)
        
        # Write the DataFrame to a CSV file
        df1.write.csv(output_path, mode="overwrite")

        print(f"Files written to {output_path}")

except Exception as e:
    print("An error occurred:", e)
    import traceback
    traceback.print_exc()

finally:
    # Stop the Spark session
    spark.stop()
