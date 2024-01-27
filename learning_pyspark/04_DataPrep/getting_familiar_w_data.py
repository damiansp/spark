from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType


DATA = '../data'

spark = SparkSession.builder.appName('Exploration').getOrCreate()
sc = spark.sparkContext


fraud = sc.textFile('ccFraud.csv.gz')
header = fraud.first()
fraud = (
    fraud
    .filter(lambda row: row != header)
    .map(lambda row: [int(e) for e in row.split(',')]))
fields = [
    *[StructField(h[1:-1], IntegerType(), True) for h in header.split(',')]]
schema = StructType(fields)
