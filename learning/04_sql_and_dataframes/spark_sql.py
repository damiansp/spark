from pyspark.sql import SparkSession


DATA = '../../data/databricks-datasets/learning-spark-v2/flights'
spark = SparkSession.builder.appName('SQL').getOrCreate()

