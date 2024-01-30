from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
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
      
