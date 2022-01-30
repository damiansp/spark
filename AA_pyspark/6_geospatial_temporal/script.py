from pyspark.sql import functions as fn, SparkSession


DATA = '../../data'
TAXI = f'{DATA}/taxidata'

spark = SparkSession.builder.appName('taxi').getOrCreate()
taxi_raw = spark.read.option('header', 'true').csv(TAXI)
taxi_raw.show(1, vertical=True)
taxi_raw.printSchema()

dt_cols = ['pickup_datetime', 'dropoff_datetime']
for dt_col in dt_cols:
    taxi_raw = taxi_raw.withColumn(
        dt_col, fn.to_timestamp(fn.col(dt_col), 'yyyy-MM-dd HH:mm:ss'))
taxi_raw.printSchema()
taxi_raw.sort(fn.col('pickup_datetime').desc()).show(3, vertical=True)

geospatial_temporal_cols = [
    'pickup_longitude', 'pickup_latitude', 'dropoff_longitude',
    'dropoff_latitude'
] + dt_cols
taxi_raw.select(
    [fn.count(fn.when(fn.isnull(c), c)).alias(c)
     for c in geospatial_temporal_cols]
).show()
taxi_raw = taxi_raw.na.drop(subset=geospatial_temporal_cols)
print('n records with 0 lat/lon vals')
taxi_raw\
    .groupBy(
        (fn.col('pickup_latitude') == 0)
        | (fn.col('pickup_longitude') == 0)
        | (fn.col('dropoff_latitude') == 0)
        | (fn.col('dropoff_longitude') == 0))\
    .count()\
    .show()
