from typing import Iterator

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, pandas_udf, udf
from pyspark.sql.types import LongType

import pandas as pd


spark = SparkSession.builder.appName('arrow_pandas').getOrCreate()


# Convert to/from Pandas
#spark.conf.set('spark.sql.exectution.arrow.pyspark.enabled', 'true')


# Pandas/Vectorized UDFs
@pandas_udf('col1 string, col2 long')
def f(s1: pd.Series, s2: pd.Series, s3: pd.DataFrame) -> pd.DataFrame:
    s3['col2'] = s1 + s2.str.len()
    return s3


df = spark.createDataFrame(
    [[1, 'a string', ('a nested string', )]],
    'long_col long, string_col string, struct_col struct<col1:string>')
df.printSchema()
df.select(f('long_col', 'string_col', 'struct_col')).printSchema()


# Series to Series
def mult_f(a: pd.Series, b: pd.Series) -> pd.Series:
    return a * b


multiply = pandas_udf(mult_f, returnType=LongType())


x = pd.Series([1, 2, 3])
print(mult_f(x, x))

df = spark.createDataFrame(pd.DataFrame(x, columns=['x']))
df.select(multiply(col('x'), col('x'))).show()
      

#@pandas_udf('long')
#def calculate(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
#    state = very_expensive_init()
#    for x in iterator:
#        yield calculate_with_state(x, state)

#df.select(calculate('value')).show()


pdf = pd.DataFrame({'x': [1, 2, 3]})
df = spark.createDataFrame(pdf)


@pandas_udf('long')
def incr(iterator: Iterator[pd.Serie]) -> Iterator[pd.Series]:
    for x in iterator:
        yield x + 1

df.select('x', incr('x')).show()


df = spark.createDataFrame(
    [(1, 1.), (1, 2.), (2, 3.), (2, 5.), (2, 10.)], ('id', 'v'))


@pandas_udf('double')
def mean_udf(v: pd.Series) -> float:
    return v.mean()


df.select(mean_udf(df.v)).show()
df.grouby('id').agg(mean_udf(df.v)).show()
w = (
    Window
    .partitionBy('id')
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
df.withColumn('mean_v', mean_udf(df.v).over(w)).show()


# Pandas Function APIs
# Grouped Map
def subtract_mean(pdf: pd.DataFrame) -> pd.DataFrame:
    v = pdf.v
    return pdf.assign(v=v - v.mean())


(df
 .groupby('id')
 .applyInPandas(subtract_mean, schema='id long, v double')
 .show())


# Map
df = spark.createDataFrame([(1, 21), (2, 30)], ('id', 'age'))


def filter_func(iterator: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
    for pdf in iterator:
        yield pdf[pdf.id == 1]


df.mapInPandas(filter_func, schema=df.schema).show()


# Co-grouped Map
df1 = spark.createDataFrame(
    [(20_000_101, 1, 1.),
     (20_000_101, 2, 2.),
     (20_000_102, 1, 3.),
     (20_000_102, 2, 4.)],
    ('time', 'id', 'v1'))
df2 = spark.createDataFrame(
    [(20_000_101, 1, 'x'), (20_000_101, 2, 'y')], ('time', 'id', 'v2'))


def merge_ordered(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    return pd.merge_ordered(left, right)


(df1
 .groupby('id')
 .cogroup(df2.groupby('id'))
 .applyInPandas(merge_ordered, schema='time int, id int, v1 double, v2 string')
 .show())


# Arrow Python UDFs
@udf(returnType='int')
def slen(s):
    return len(s)


@udf(returnType='int', useArrow=True)
def arrow_slen(s):
    return len(s)


df = spark.createDataFrame([(1, 'Johnny D', 32)], ('id', 'name', 'age'))
df.select(slen('name'), arrow_slen('name')).show()



