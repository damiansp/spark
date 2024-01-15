from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count, countDistinct, mean, monotonically_increasing_id)


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

df_missing = spark.createDataFrame(
    [(1, 144.5, 5.9, 33, 'M', 100_000),
     (2, 167.2, 5.4, 45, 'M', None),
     (3, None, 5.2, None, None, None),
     (4, 144.5, 5.9, 33, 'M', None),
     (5, 133.2, 5.7, 54, 'F', None),
     (3, 124.1, 5.2, 23, 'F', None),
     (5, 129.2, 5.3, 42, 'M', 76_000)],
    ['id', 'weight', 'height', 'age', 'gender', 'income'])
print(
    df_missing
    .rdd
    .map(lambda row: (row['id'], sum([c is None for c in row])))
    .collect())
# pct missing per col
df_missing.agg(
    *[(1 - count(c) / count('*')).alias(f'{c}_missing')
      for c in df_missing.columns]
).show()
# drop col
df_missing = df_missing.select(
    [c for c in df_missing.columns if c != 'income'])
df_missing.dropna(thresh=3).show()

means = (
    df_missing
    .agg(*[mean(c).alias(c) for c in df_missing.columns if c != 'gender'])
    .toPandas()
    .to_dict('records')[0])
means['gender'] = 'missing'
df_missing.fillna(means).show()
