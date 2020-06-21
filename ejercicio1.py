from pyspark.sql import SparkSession  

scSpark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example: Reading CSV file without mentioning schema") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sdfData = scSpark.read.csv("/home/oswald/data/all_data.csv", header=True, sep=",")
sdfData.show()