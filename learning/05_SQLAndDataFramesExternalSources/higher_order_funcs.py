from pyspark.sql.types import ArrayType, IntegerType, StructType


schema = StructType([StructField('celsius', ArrayType(IntegerType()))])
t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
t_c = spark.createDataFrame(t_list, schema)
t_c.createOrReplaceTempView('tC')
t_c.show()

spark.sql(
    'SELECT celsius, '
    '  transform(celsius, t -> ((9*t) DIV 5) + 32) AS fahrenheit, '
    '  filter(celsius, t -> t > 38) AS highs, '
    '  exists(celsius, t -> t > 38) AS has_highs, '
    '  reduce('
    '    celsius,'
    '    0,'
    '    (t, acc) -> t + acc,'
    '    acc -> (acc DIV SIZE(celsius) * 9 DIV 5) + 32'
    '  ) AS avg_temp_f '
    'FROM tC'
).show()


