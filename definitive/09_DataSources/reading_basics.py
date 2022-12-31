(spark
 .read
 .format('csv')
 .option('mode', 'FAILFAST')
 .option('inferSchema', 'true')
 .schema(my_schema)
 .load())
