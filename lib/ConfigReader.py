import configparser
from pyspark import SparkConf

# Loading the application configs in Python dictionary
def get_app_config(env):
    config = configparser.ConfigParser()
    config.read("configs/application.conf")
    app_conf = {}
    for key, val in config.items(env):
        app_conf[key] = val
    return app_conf

# Loading the PySpark configs and creating a SparkConf object
def get_pyspark_config(env):
    config = configparser.ConfigParser()
    config.read("configs/pyspark.conf")
    pyspark_conf = SparkConf()
    for key, val in config.items(env):
        pyspark_conf.set(key, val)
    return pyspark_conf
