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
