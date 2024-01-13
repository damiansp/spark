from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


'''
schema = StructType([
    StructField('author', StringType(), False),
    StructField('title', StringType(), False),
    StructField('pages', IntegerType(), False)])
simple_schema = 'author STRING, title STRING, pages INT'
'''

# fuller example
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
    blogs_df.show()
    blogs_df.printSchema()
    print(blogs_df.schema)
