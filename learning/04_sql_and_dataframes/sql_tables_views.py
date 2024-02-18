from pyspark.sql import SparkSession


DATA = '../../data/databricks-datasets/learning-spark-v2'
spark = SparkSession.builder.appName('DBs').getOrCreate()


# Creating DBs and Tables
#spark.sql('CREATE DATABASE learn_spark')
#spqrk.sql('USE learn_spark')


# Create a managed table
schema = (
    'date STRING, delay INT, distance INT, origin STRING, destination STRING')
spark.sql(f'CREATE TABLE flights ({schema})')
data_file = f'{DATA}/flights/departuredelays.csv')
flights_df = spark.read.csv(data_file, schema=schema)
flights_df.wite.saveAsTable('flights')


# Create an unmanaged table
spark.sql(
    f'''CREATE TABLE flights_unmng({schema})
    USING csv
    OPTIONS (PATH {data_file})''')
(flights_f
 .write.options('path', '/tmp/data/flights_unmng')
 .saveAsTable('flights_unmng'))


# Creating Views
sfo_df = spark.sql(
    "SELECT date, delay, origin, destination "
    "FROM flights_unmng "
    "WHERE origin = 'SFO'")
jfk_df = spark.sql(
    "SELECT date, delay, origin, destination "
    "FROM flights_unmng "
    "WHERE origin = 'JFK'")
sfo_df.createOrReplaceGlobalTempView('sfo')
jfk_df.createOrReplaceTempView('jfk')

spark.sql('SELECT * FROM jfk LIMIT 10').show()
spark.catalog.dropGlobalTempView('sfo')
spark.catalog.dropTempView('jfk')


# Viewing metadata
spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.listColumns('flights')


# Reading tables into DFs
us_flights_df = spark.sql('SELECT * FROM flights')
us_flights_df2 = spark.table('flights')
