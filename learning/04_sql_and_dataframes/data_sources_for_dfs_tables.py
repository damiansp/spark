from pyspark.sql import SparkSession


# DF Reader
spark = SparkSession.builder.appName('DFSources').getOrCreate()
parquet_dir = 'my/path/to/myfile.parquet/'
df = spark.read.format('parquet').load(parquet_dir)
# since parquet is default, above can be just
df = spark.read.load(parquet_file)

csv_dir = 'my/path/to/my_csv_dir/*'
df = (
    spark.read.format('csv')
    .option('inferSchema', 'true')
    .option('header', 'true')
    .option('mode', 'PERMISSIVE')
    .load(my_csv_dir))

json_dir = 'my/path/to/my_json_dir/*'
df = spark.read.format('json').load(json_dir)


# DF Writer
df.write.format('json').mode('overwrite').save('my/location') #.partitionBy()


# Parquet
# Reading to DF
df = spark.read.format('parquet').load(parquet_dir)


# Read Parquet to SQL Table
#CREATE OR REPLACE TEMPORARY VIEW flights
#USING parquet
#OPTIONS (path "my/path/to/myfile.parquet/")
spark.sql('SELECT * FROM flights').show()


# Write DF to Parquet
(df
 .write.format('parquet')  # .format('parquet') is default/optional
 .mode('overwrite')
 .option('compression', 'snappy')
 .save('/tmp/data/parquet'))


# Write DF to SQL Table
df.write.mode('overwrite').saveAsTable('flights')
