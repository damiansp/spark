from pyspark.sql import SparkSession


DATA = '../../data/databricks-datasets/learning-spark-v2/flights'
spark = SparkSession.builder.appName('SQL').getOrCreate()
path = f'{DATA}/departuredelays.csv'
df = (
    spark
    .read.format('csv')
    .option('inferSchema', 'true')
    .option('header', 'true')
    .load(path))
df.createOrReplaceTempView('delays')

spark.sql(
    '''SELECT distance, origin, destination
    FROM delays
    WHERE distance > 1000
    ORDER BY distance DESC'''
).show(10)

spark.sql(
    '''SELECT date, delay, origin, destination
    FROM delays
    WHERE delay > 120 AND origin = 'SFO'
    ORDER BY delay DESC'''
).show(10)
    
