from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType


udf_example_df = spark.range(5).toDF('n')


def cube(dub):
    return dub ** 3

cube(2.)

cube_udf = udf(cube)
udf_example_df.select(cube_udf(col('n'))).show()
udf_example_df.selectExpr('cube(n)').show()

spark.udf.register('cube', cube, DoubleType())
udf_example_df.selectExpr('cube(n)').show2
