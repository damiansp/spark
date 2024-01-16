from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.types import (
    BooleanType, FloatType, IntegerType, StringType, StructField, StructType)


DATA = '../../data'


spark = SparkSession.builder.appName('Fires').getOrCreate()
fire_schema = StructType([
    StructField('call_number', IntegerType(), True),
    StructField('unit_id', StringType(), True),
    StructField('incident_number', IntegerType(), True),
    StructField('call_type', StringType(), True),
    StructField('call_date', StringType(), True),
    StructField('watch_date', StringType(), True),
    StructField('call_final_disposition', StringType(), True),
    StructField('available_dt_tm', StringType(), True),
    StructField('address', StringType(), True),
    StructField('city', StringType(), True),
    StructField('zipcode', StringType(), True),
    StructField('battalion', StringType(), True),
    StructField('staion_area', StringType(), True),
    StructField('box', StringType(), True),
    StructField('original_priority', StringType(), True),
    StructField('priority', StringType(), True),
    StructField('final_priority', StringType(), True),
    StructField('is_als_unit', BooleanType(), True),
    StructField('call_type_group', StringType(), True),
    StructField('n_alarms', IntegerType(), True),
    StructField('unit_type', StringType(), True),
    StructField('unit_sequence_in_call_dispatch', IntegerType(), True),
    StructField('fire_prevention_district', StringType(), True),
    StructField('supervision_district', StringType(), True),
    StructField('neighborhood', StringType(), True),
    StructField('location', StringType(), True),
    StructField('row_id', StringType(), True),
    StructField('delay', FloatType(), True)])
path = f'{DATA}/sf-fire/sf-fire-calls.csv'
fire_df = spark.read.csv(path, header=True, schema=fire_schema)
fire_df.show()


# Save (parquet)
#outpath = f'{DATA}/sf-fire/parquet'
#fire_df.write.format('parquet').save(outpath)

#parquet_table = 'myschema.mytable'
#fire_df.write.format('parquet').saveAsTable(parquet_table)


# projections/filters
few_fire_df = (
    fire_df
    .select('incident_number', 'available_dt_tm', 'call_type')
    .where(col('call_type') != 'Medical Incident'))
few_fire_df.show(5, truncate=False)
(fire_df
 .select('call_type')
 .where(col('call_type').isNotNull())
 .agg(countDistinct('call_type').alias('distinct_call_types'))
 .show())

