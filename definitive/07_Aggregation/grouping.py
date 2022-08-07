from pyspark.sql.functions import count


df.groupBy('invoice', 'customer_id').count().show()
(df
 .groupBy('invoice')
 .agg(count('Quantity').alias('qty'), expr('count(Quantity'))
 .show())

df.groupBy('invoice').agg(expr('avg(Quantity)'), expr('stddev_pop(Quantity)'))
