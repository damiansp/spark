from pyspark.sql.functions import count, countDistinct

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


