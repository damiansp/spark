from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, column, columns, expr, lit
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


# DF Transformations
df = spark.read.format('json').load(path)
df.createOrReplaceTempView

my_schema = StructType([
    StructField('some', StringType(), True),
    StructField('col', StringType(), True),
    StructField('names', LongType(), False)])
my_row = Row('Hello', None, 1)
my_df = spark.createDataFrame([my_row], my_schema)
my_df.show()

df.select('DEST_COUNTRY_NAME', 'ORIGIN_COUNTRY_NAME').show(2)
df.select(
    expr('DEST_COUNTRY_NAME'),
    col('DEST_COUNTRY_NAME'),
    column('DEST_COUNTRY_NAME')
).show(2)
df.select(expr('DEST_COUNTRY_NAME AS dest')).show(2)
df.select(expr('DEST_COUNTRY_NAME AS dest').alias('DEST_COUNTRY_NAME')).show(2)
df.selectExpr('DEST_COUNTRY_NAME AS newColName', 'DEST_COUNTRY_NAME').show(2)
df.selectExpr(
    '*', 'DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) AS withinCountry'
).show(2)
df.selectExpr('AVG(count)', 'COUNT(DISTINCT(DEST_COUNTRY_NAME))').show(2)


# Converting to Spark Types
df.select(expr('*'), lit(1).alias('One')).show(2)


# Adding Columns
df.withColumn('intercept', lit(1)).show(2)
df.withColumn('domestic', expr('ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME'))
df.withColumn('dest', expr('DEST_COUNTRY_NAME')).columns  # rename


# Renaming Columns
df.withColumnRenamed('DEST_COUNTRY_NAME', 'dest').columns


# Reserved Chars/Keywords
df_with_long_col_name = df.withColumn(
    'This Long Column-Name', expr('ORIGIN_COUNTRY_NAME'))
df_with_long_col_name.selectExpr(
    '`This Long Column-Name`', '`This Long Column-Name` AS `new col`')


# Removing Columns
df.drop('ORIGIN_COUNTRY_NAME').columns
df.drop('ORIGIN_COUNTRY_NAME', 'DEST_COUNTRY_NAME')
