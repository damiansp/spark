from pyspark.sql.functions import lit


df = (
    spark.read.format('csv')
    .option('header', 'true')
    .option('inferSchema', 'true')
    .load('my/path/to/file.csv'))
df.printSchema()
df.createOrReplaceTempView('table')

df.select(lit(5), lit('five'), lit(5.))
