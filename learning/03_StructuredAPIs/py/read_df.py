from pyspark.sql.types import *

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
    StructField('Battalion' StringType(), True),
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
    '/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv')
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
