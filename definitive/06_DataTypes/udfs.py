from pyspark.sql.functions import col, udf


udf_example_df = spark.range(5).toDF('n')


def cube(dub):
    return dub ** 3

cube(2.)

cube_udf = udf(cube)
udf_example_df.select(cube_udf(col('n'))).show()
udf_example_df.selectExpr('cube(n)').show()
