from lib import ConfigReader
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Defining customer schema
# def get_customers_schema():
#     schema = "customer_id int, customer_fname string, customer_lname string, username string, password string, address string, city string, state string, pincode string"
#     return StructType([StructField(field.split()[0], getattr(StringType(), field.split()[1].capitalize()), True) for field in schema.split(",")])


def get_customers_schema():
    schema = "customer_id int, customer_fname string, customer_lname string, username string, password string, address string, city string, state string, pincode string"
    fields = [field.strip() for field in schema.split(",")]
    return StructType([StructField(field.split()[0], StringType() if 'string' in field else IntegerType(), True) for field in fields])


# Creating customers DataFrame
def read_customers(spark, env):
    conf = ConfigReader.get_app_config(env)
    customers_file_path = conf["customers.file.path"]
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_customers_schema()) \
        .load(customers_file_path)

# Defining orders schema
#def get_orders_schema():
    #schema = "order_id int, order_date string, customer_id int, order_status string"
    #return StructType([StructField(field.split()[0], getattr(StringType(), field.split()[1].capitalize()), True) for field in schema.split(",")])


def get_orders_schema():
    schema = "order_id int, order_date string, customer_id int, order_status string"
    fields = [field.strip() for field in schema.split(",")]
    return StructType([StructField(field.split()[0], StringType() if 'string' in field else IntegerType(), True) for field in fields])


# Creating orders DataFrame
def read_orders(spark, env):
    conf = ConfigReader.get_app_config(env)
    orders_file_path = conf["orders.file.path"]
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(get_orders_schema()) \
        .load(orders_file_path)
