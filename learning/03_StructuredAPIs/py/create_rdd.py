from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

data = [
    ('Bobby', 20), ('Denny', 31), ('Jules', 30), ('Elmer', 64), ('Bobby', 25)]
# what is sc???
#data_rdd = sc.parallelize(data)
#    
#ages_rdd = (data_rdd
#            .map(lambda x: (x[0], (x[1], 1)))
#            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
#            .map(lambda x: (x[0], x[1][0] / x [1][1])))

# Equivalent using DataFrame
spark = SparkSession.builder.appName('PeopleAges').getOrCreate()
data_df = spark.createDataFrame(data, ['name', 'age'])
avg_df = data_df.groupBy('name').agg(avg('age'))
avg_df.show()
