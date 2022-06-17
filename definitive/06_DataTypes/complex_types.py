from pyspark.sql.functions import array_contains, explode, size, split, struct


df.selectExpr('(code, idn) AS complex', '*')
df.selectExpr('struct(code, idn) AS complex', '*')
complex_df = df.select(struct('code', 'idn').alias('complex'))
complex_df.createOrReplaceTempView('complex_table')
complex_table.select('complex.code')
complex_table.select(col('complex').getField('idn'))

df.select(split(col('field'), ' ')).show(2)
(df
 .select(split(col('field', ' ')).alias('array_col'))
 .selectExpr('array_col[0]')
 .show(2))

df.select(size(split(col('field'), ' '))).show(s)

df.select(array_contains(split(col('field'), ' '), 'WHITE')).show(2)

(df
 .withColumn('split', split(col('field'), ' '))
 .withColumn('exploded', explode(col('split')))
 .select('field', 'other_field', 'exploded')
 .show(2))

