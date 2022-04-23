from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import asc, col, column, columns, desc, expr, lit
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


# Type Casting
df.withColumn('count2', col('count').cast('long'))


# Filtering Rows
df.filter(col('count') < 2).show(2)
df.where('count < 2').show(2)
df.where(col('count') < 2).where(col('ORIGIN_COUNTRY_NAME') != 'Croatia')


# Getting Unique Rows
df.select('ORIGIN_COUNTRY_NAME').distinct().count()


# Random Samples
seed = 5
with_replacement = False
frac = 0.2
df.sample(with_replacement, frac, seed)


# Random Splits
dfs = df.randomSplit([0.25, 0.75], seed)
dfs[0].count()


# Concatenating and Appending Rows
schema = df.schema
new_rows = [
    Row('New Country', 'Other Country', 5L),
    Row('New Country 2', 'Other Country 3', 1L)]
parallelized_rows = spark.sparkContext.parallelize(new_rows)
new_df = spark.createDataFrame(parallelized_rows, schema)

# or
(df
 .union(new_df)
 .where('count = 1')
 .where(col('ORIGIN_COUNTRY_NAME') != 'United States')
 .show())


# Sorting Rows
df.sort('count').show(5)
df.orderBy('count', 'DEST_COUNTRY_NAME').show(5)
df.orderBy(col('count'), col('DEST_COUNTRY_NAME')).show(5)

df.orderBy(expr('count desc')).show(2)
df.orderBy(col('count').desc(), col('DEST_COUNTRY_NAME').asc()).show(2)

(spark
 .read
 .format('json')
 .load('my/data/path.json')
 .sortWithinPartitions('count'))


# Limit
df.limit(5).show()
df.orderBy(expr('count desc')).limit(6).show()
