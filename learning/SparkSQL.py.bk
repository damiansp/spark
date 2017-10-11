from pyspark.sql import HiveContext, Row # or
from pyspark.sql import SQLContext, Row  # (if can't include hive dependencies)

sc = SparkContext(...)
hive_ctx = HiveContext(sc) # or:
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


# From RDDs
happy_people_RDD = sc.parallelize([Row(name='REM', favorite_drink='coffee')])
happy_people_DF = hive_ctx.inferSchema(happy_people_RDD)
happy_people_DF.registerTempTable('happy_people')


# UDFs (User-Defined Functions)
