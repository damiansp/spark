from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count, countDistinct, monotonically_increasing_id)


spark = SparkSession.builder.appName('Dupes Missing Outliers').getOrCreate()


df = spark.createDataFrame(
    [(1, 144.5, 5.9, 33, 'M'),
     (2, 167.2, 5.4, 45, 'M'),
     (3, 124.1, 5.2, 23, 'F'),
     (4, 144.5, 5.9, 33, 'M'),
     (5, 133.2, 5.7, 54, 'F'),
     (3, 124.1, 5.2, 23, 'F'),
     (5, 129.2, 5.3, 42, 'M')],
    ['id', 'weight', 'height', 'age', 'gender'])
print(f'N rows: {df.count()}')
print(f'N distinct rows: {df.distinct().count()}')

df = df.dropDuplicates()
print(f'N rows: {df.count()}')
n_ids = df.select([c for c in df.columns if c != 'id']).distinct().count()
print(f'N unique IDs: {n_ids}')

df = df.dropDuplicates(subset=[c for c in df.columns if c != 'id'])
(df
 .agg(count('id').alias('count'), countDistinct('id').alias('distinct'))
 .show())
df.withColumn('new_id', monotonically_increasing_id()).show()
