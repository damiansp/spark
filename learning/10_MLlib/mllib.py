from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


spark = SparkSession.builder.appName('Common Ops').getOrCreate()
DATA = '../../data/databricks-datasets/learning-spark-v2'


file_path = f'{DATA}/sf-airbnb/sf-airbnb-clean.parquet'
airbnb_df = spark.read.parquet(file_path)
airbnb_df.select(
    'neighbourhood_cleansed', 'room_type', 'bedrooms', 'bathrooms',
    'number_of_reviews', 'price'
).show(5)
