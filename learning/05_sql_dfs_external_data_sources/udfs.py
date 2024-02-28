import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, udf
from pyspark.sql.types import LongType


spark = SparkSession.builder.appName('udfs').getOrCreate()


schema = 'id INT'
data = [[1], [2], [3], [4], [5]]
df = spark.createDataFrame(data, schema)


def cube(n):
    return n * n * n


@udf(returnType=LongType())
def square(n):
    return n * n

spark.udf.register('cube', cube, LongType())
spark.range(1, 9).createOrReplaceTempView('udf_test')
spark.sql('SELECT id, cube(id) AS i3 FROM udf_test').show()

df = df.withColumn('i2', square(col('id')))
df.show()


def fourth(n: pd.Series) -> pd.Series:
    sq = n * n
    return sq * sq


fourth_udf = pandas_udf(fourth, returnType=LongType())
df.select('id', fourth_udf(col('id'))).show()
