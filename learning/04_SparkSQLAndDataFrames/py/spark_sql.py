from pyspark.sql import SparkSession


DATA = '../../../data'

spark = SparkSession.builder.appName('SparkSQL').getOrCreate()
data_path = (
    f'{DATA}/databricks-datasets/learning-spark-v2/flights/departuredelays.csv')
schema = (
    '`date` STRING, `delay` INT, `distance` INT, `origin` STRING, '
    '`destination` STRING')
df = (spark.read.format('csv')
      .option('inferSchema', 'true')
      .option('header', 'true')
      .load(data_path))
df.createOrReplaceTempView('flight_delays')
