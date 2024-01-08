from pyspark.sql import SparkSession
from pyspark.sql.functions import avg


spark = SparkSession.builder.appName('AuthorsAges').getOrCreate()
df = spark.createDataFrame(
    [('Brooke', 20), ('Denny', 31), ('Jules', 30), ('TD', 35), ('Brooke', 25)],
    ['name', 'age'])
avg_df = df.groupBy('name').agg(avg('age'))
avg_df.show()
