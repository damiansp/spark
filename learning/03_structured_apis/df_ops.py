from pyspark.sql import SparkSession
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
