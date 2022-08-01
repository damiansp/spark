from pyspark.sql.functions import (
    approx_count_distinct, avg, collect_list, collect_set,  corr, count,
    countDistinct, covar_pop, covar_samp, expr, first, kurtosis, last,
    max as sql_max, min as sql_min, skewness, stddev_pop, stddev_samp,
    sum as sql_sum, sumDistinct, var_pop, var_samp)

df = (
    spark
    .read
    .format('csv')
    .option('header', 'true')
    .option('inferSchema', 'true')
    .load('/my/path/*.csv')
    .coalesce(5))
df.cache()
df.getOrReplaceTempView('df')

print(df.count())
df.select(count('StockCode')).show()  # SELECT COUNT(*) FROM df
df.select(countDistinct('StockCode')).show()
# SELECT COUNT(DISTINCT StockCode) FROM df


df.select(approx_count_distinct('StockCode', 0.1)).show()
df.select(first('StockCode'), last('StockCode')).show()
df.select(sql_min('Quantity'), sql_max('Quantity')).show()
df.select(sql_sum('Quantity')).show()
df.select(sumDistinct('Quantity')).show()

(df
 .select(
     count('Quantity').alias('n_transactions'),
     sql_sum('Quantity').alias('total_sales'),
     avg('Quantity').alias('mean_sale'),
     expr('mean(Quantity)').alias('mean_sale_again'))
 .selectExpr(
     'total_sales / n_transactions', 'mean_sale', 'mean_sale_again')
 .show())

df.select(
    var_pop('Quantity'),
    var_samp('Quantity'),
    stddev_pop('Quantity'),
    stddev_samp('Quantity'))
df.select(skewness('Quality'), kurtosis('Quality'))
df.select(
    corr('Price', 'Quantity'),
    covar_samp('Price', 'Quantity'),
    covar_pop('Price', 'Quanity'))
df.agg(collect_set('Country'), collect_set('Country'))
