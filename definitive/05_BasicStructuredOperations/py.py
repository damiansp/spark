from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, columns, expr
from pyspark.sql.types import LongType, StringType, StructField, StructType


DATA = '../../data'
path = f'{DATA}/flight-data/json/2015-summary.json'


# Schemata
spark = SparkSession.builder.appName('flights').getOrCreate()
df = spark.read.format('json').load(path)
df.printSchema()

print(
    spark.read.format('json')
    .load(f'{DATA}/flight-data/json/2015-summary.json')
    .schema)

my_schema = StructType([
    StructField('DEST_COUNTRY_NAME', StringType(), True),
    StructField('ORIGIN_COUNTRY_NAME', StringType(), True),
    StructField('count', LongType(), False, metadata={'hello': 'world'})])
df = spark.read.format('json').schema(my_schema).load(path)


# Columns and Expressions
col('my_column')
column('my_column')
df.col('count')

(((col('my_col') + 5) * 200) - 6) < col('other_col')
expr('(((my_col + 5) * 200) - 6) < other_col')

spark.read.format('json').load(path).columns


# Records and Rows
df.first()

my_row = Row('Hello', None, 1, False)
my_row[0]
my_row[2]
