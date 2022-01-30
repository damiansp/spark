from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *


DATA = '../../../data'
spark = SparkSession.builder.appName('df_api').getOrCreate()
fire_schema = StructType([
    StructField('CallNumber', IntegerType(), True),
    StructField('UnitId', StringType(), True),
    StructField('IncidentNumber', IntegerType(), True),
    StructField('CallType', StringType(), True),
    StructField('CallDate', StringType(), True),
    StructField('WatchDate', StringType(), True),
    StructField('CallFinalDisposition', StringType(), True),
    StructField('AvailableDtTm', StringType(), True),
    StructField('Address', StringType(), True),
    StructField('City', StringType(), True),
    StructField('Zipcode', IntegerType(), True),
    StructField('Battalion', StringType(), True),
    StructField('StationArea', StringType(), True),
    StructField('Box', StringType(), True),
    StructField('OriginalPriority', StringType(), True),
    StructField('Priority', StringType(), True),
    StructField('FinalPriority', StringType(), True),
    StructField('ALSUnit', BooleanType(), True),
    StructField('CallTypeGroup', StringType(), True),
    StructField('NumAlarms', IntegerType(), True),
    StructField('UnitType', StringType(), True),
    StructField('UnitSequenceInCallDispatch', IntegerType(), True),
    StructField('FirePreventionDistrict', StringType(), True),
    StructField('SupervisorDistrict', StringType(), True),
    StructField('Neighborhood', StringType(), True),
    StructField('Location', StringType(), True),
    StructField('RowID', StringType(), True),
    StructField('Delay', FloatType(), True)])
sf_fire_file = (
    f'{DATA}/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv')
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
parquet_path = sf_fire_file.replace('.csv', '.parquet')
fire_df.write.format('parquet').save(parquet_path)
# as table
parquet_table = 'sf_fires'
fire_df.write.format('parquet').saveAsTable(parquet_table)


# Transformations/Actions
few_fire_df = (fire_df
               .select('IncidentNumber', 'AvailableDtTm', 'CallType')
               .where(col('CallType') != 'Medical Incident'))
few_fire_df.show(5, truncate=False)

fire_df.select('CallType')\
       .where(col('CallType').isNotNull())\
       .agg(countDistinct('CallType').alias('DistinctCallTypes'))\
       .show()

fire_df.select('CallType')\
       .where(col('CallType').isNotNull())\
       .distinct()\
       .show(10, False)

new_fire_df = fire_df.withColumnRenamed('Delay', 'ResponseDelayInMins')
new_fire_df.select('ResponseDelayInMins')\
           .where(col('ResponseDelayInMins') > 5)\
           .show(5, False)

fire_ts_df = (
    new_fire_df
    .withColumn('IncidentDate', to_timestamp(col('CallDate'), 'MM/dd/yyyy'))
    .drop('CallDate')
    .withColumn('OnWatchDate', to_timestamp(col('WatchDate'), 'MM/dd/yyyy'))
    .drop('WatchDate')
    .withColumn('AvailableDtTS',
                to_timestamp(col('AvailableDtTm'), 'MM/dd/yyyy hh:mm:ss a'))
    .drop('AvailableDtTm'))
fire_ts_df.select('IncidentDate', 'OnWatchDate', 'AvailableDtTS').show(5, False)
fire_ts_df\
    .select(year('IncidentDate'))\
    .distinct()\
    .orderBy(year('IncidentDate'))\
    .show()

# Aggregate
fire_ts_df\
    .select('CallType')\
    .where(col('CallType').isNotNull())\
    .groupBy('CallType')\
    .count()\
    .orderBy('count', ascending=False)\
    .show(n=10, truncate=False)

fire_ts_df.select(
    F.sum('NumAlarms'),
    F.avg('ResponseDelayInMins'),
    F.min('ResponseDelayInMins'),
    F.max('ResponseDelayInMins')
).show()
