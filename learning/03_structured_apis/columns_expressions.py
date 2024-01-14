from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr


schema = (
    'id INT NOT NULL, first STRING, last STRING, url STRING, '
    'published STRING, hits INT, Campaigns ARRAY<STRING>')
data = [
    [1, 'Jules', 'Damji', 'https://tinyurl.1', '1/4/2016', 435,
     ['twitter', 'LinkedIn']],
    [2, 'Brooke', 'Wenig', 'https://tinyurl.2', '5/5/2018', 8909,
     ['twitter', 'LinkedIn']],
    [3, 'Denny', 'Lee', 'https://tinyurl.3', '6/7/2019', 7659,
     ['web', 'twitter', 'fb', 'LinkedIn']],
    [4, 'Tathagata', 'Das', 'https://tinyurl.4', '5/12/2018', 10568,
     ['twitter', 'fb']]]


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Schema').getOrCreate()
    blogs_df = spark.createDataFrame(data, schema)
    print(blogs_df.columns)
    print(blogs_df['id'])
    blogs_df.select(expr('hits * 2')).show()
    blogs_df.select(col('hits') * 2).show()
    blogs_df.withColumn('big_hitter', col('hits') > 10_000).show()
