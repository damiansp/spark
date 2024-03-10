from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, IntegerType, StructField, StructType


spark = SparkSession.builder.appName('HOFs').getOrCreate()
schema = StructType([StructField('celsius', ArrayType(IntegerType()))])
t_list = [[35, 36, 32, 40, 40, 42, 38]], [[31, 32, 34, -40, 56]]
t_c = spark.createDataFrame(t_list, schema)
t_c.createOrReplaceTempView('tC')
t_c.show()


# transform()
spark.sql(
    '''SELECT
      celsius,
      transform(celsius, t -> ((t * 9) div 5) + 32) AS fahrenheit
    FROM tC'''
).show()


# filter()
spark.sql(
    '''SELECT celsius, filter(celsius, t -> t > 38) AS high
    FROM tC'''
).show()


# exists()
spark.sql(
    '''SELECT celsius, exists(celsius, t -> t = 38) AS threshold
    FROM tC'''
).show()

