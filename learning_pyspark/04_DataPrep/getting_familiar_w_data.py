from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType


DATA = '../data'

spark = SparkSession.builder.appName('Exploration').getOrCreate()
sc = spark.sparkContext


fraud = sc.textFile(f'{DATA}/ccFraud.csv.gz')
header = fraud.first()
fraud = (
    fraud
    .filter(lambda row: row != header)
    .map(lambda row: [int(e) for e in row.split(',')]))
fields = [
    *[StructField(h[1:-1], IntegerType(), True) for h in header.split(',')]]
schema = StructType(fields)
fraud_df = spark.createDataFrame(fraud, schema)
fraud_df.printSchema()

fraud_df.groupby('gender').count().show()
numerics = ['balance', 'numTrans', 'numIntlTrans']
fraud_df.describe(numerics).show()
fraud_df.agg({'balance': 'skewness'}).show()


# Correlation
