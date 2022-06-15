from pyspark.sql.functions import struct


df.selectExpr('(code, idn) AS complex', '*')
df.selectExpr('struct(code, idn) AS complex', '*')
complex_df = df.select(struct('code', 'idn').alias('complex'))
complex_df.createOrReplaceTempView('complex')
