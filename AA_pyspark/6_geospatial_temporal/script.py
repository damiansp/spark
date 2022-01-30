from pyspark.sql import SparkSession


DATA = '../../data'
TAXI = f'{DATA}/taxidata'

spark = SparkSession.builder.appName('taxi').getOrCreate()
taxi_raw = spark.read.option('header', 'true').csv(TAXI)
taxi_raw.show(1, vertical=True)
taxi_raw.printSchema()
