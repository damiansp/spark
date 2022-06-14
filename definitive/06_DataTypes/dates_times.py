from pyspark.sql.functions import (
    current_date, current_timestamp, date_add, date_diff, date_sub,
    months_between, to_date)


date_df = (
    spark
    .range(10)
    .withColumn('today', current_date())
    .withColumn('now', current_timestamp()))
date_df.createOrReplaceTempView('dateTable')
date_df.printSchema()

date_df.select(date_sub(col('today'), 5), date_add(col('today'), 5)).show(1)

(date_df
 .withColumn('week_ago', date_sub(col('today'), 7))
 .select(datediff(col('week_ago'), col('today')))
 .show(1))
(date_df
 .select(
     to_date(lit('2016-01-01')).alias('start'),
     to_date(lit('2017-05-22')).alias('end'))
 .select(months_between(col('start'), col('end')))
 .show(1))
