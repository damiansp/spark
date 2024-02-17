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
 .write.options('path', '/tmp/data/flights')
 .saveAsTable('flights_unmng'))



# Creating Views

