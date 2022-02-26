from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc


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

spark.sql('''
    SELECT distance, origin, destination
    FROM flight_delays
    WHERE distance > 1000
    ORDER BY distance DESC'''
).show(10)

spark.sql('''
    SELECT date, delay, origin, destination
    FROM flight_delays
    WHERE delay > 120 AND origin = 'SFO' AND destination = 'ORD'
    ORDER BY delay DESC'''
).show(10)

spark.sql('''
    SELECT delay, 
      origin,
      destination,
      CASE
        WHEN delay >= 360 THEN 'Very Long Delay'
        WHEN delay >= 120 AND delay < 360 THEN 'Long Delay'
        WHEN delay >= 60  AND delay < 120 THEN 'Short Delay'
        WHEN delay >  0   AND delay < 60  THEN 'Tolerable Delay'
        WHEN delay = 0 THEN 'No Delay'
        ELSE 'Early'
      END AS flight_delay
    FROM flight_delays
    ORDER BY origin, delay DESC'''
).show(10)

(df.select('distance', 'origin', 'destination')
 .where(col('distance') > 1000)
 .orderBy(desc('distance'))
).show(10)

(df.select('distance', 'origin', 'destination')
 .where('distance > 1000')
 .orderBy('distance', ascending=False)
).show(10)
