from pyspark.sql import HiveContext, Row # or
from pyspark.sql import SQLContext, Row  # (if can't include hive dependencies)

sc = SparkContext(...)
hiveCtx = HiveContext(sc) # or:
sqlContext = SQLContextSingleton.getInstance(sc)


# Basic Query Example
input = hiveCtx.jsonFile(input_file)
input.registerTempTable('tweets')
top_tweets = hiveCtx.sql(
    '''
    SELECT text, retweetCount
    FROM tweets
    ORDER BY retweetCount
    LIMIT 10
    ''')


# Dataframes
# Common operations
df.show()
df.select('name', df('age') + 1)
df.filter(df('age') > 19)
df.groupBy(df('name')).min() # max, mean, agg, etc

# Convert DF to RDD
top_tweet_text = top_tweets.rdd().map(lambda row: row.text)



# Loading and Saving Data
# Apache Hive
hive_ctx = HiveContext(sc)
rows = hive_ctx.sql("SELECT key, valye FROM my_table")
keys = rows.map(lambda row: row[0])

# Data Sources/Parquet
rows = hive_ctx.load(parquet_file, 'parquet')
names = rows.map(lambda row: row.name) # where "name" is a field in the table
print('Everyone:')
print(names.collect())

# Parquet query
tbl = rows.registerTempTable('people')
panda_friends = hive_ctx.sql(
    'SELECT name FROM people WHERE favorite_animal = "panda"')
print('Panda friends:')
print(panda_friends.map(lambda row: row.name).collect())

# Save
panda_friends.save('hdfs://....', 'parquet')


# JSON
# Example inputs:
{'name': 'Spot'}
{'name': 'Sparky the Bear',
 'loves_pandas': True,
 'knows': {'friends': ['Spot']} }

# Loading JSON
dat = hive_ctx.jsonFile(input_file)
