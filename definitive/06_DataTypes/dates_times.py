from pyspark.sql.functions import (
    current_date, current_timestamp, date_add, date_diff, date_sub, lit,
    months_between, to_date, to_timestamp)


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

(spark
 .range(5)
 .withColumn('date', lit('2017-01-01'))
 .select(to_date(col('date'))).show(1))

date_df.select(to_date(lit('2016-20-12')), to_date(lit('2017-12-11'))).show(1)

date_format = 'yyyy-dd-MM'
clean_date_df = (
    spark
    .range(1)
    .select(
        to_date(lit('2017-12-11'), date_format).alias('date'),
        to_date(lit('2017-20-12'), date_format).alias('date2')))
clean_date_df.createOrReplaceTempView('date_table')
clean_date_df.select(to_timestamp(col('date')), date_format)).show()
clean_date_df.filter(col('date2') > lit('2017-12-12')).show()
