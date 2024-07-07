from pyspark.ml.feature import VectorAssembler
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


train_df, test_df = airbnb_df.randomSplit([0.8, 0.2], seed=123)
print(f'Train: {train_df.count()}, Test: {test_df.count()}')

vec_assembler = VectorAssembler(inputCols=['bedrooms'], outputCol='features')
vec_train_df = vec_assembler.transform(train_df)
vec_train_df.select('bedrooms', 'features', 'price').show(10)
