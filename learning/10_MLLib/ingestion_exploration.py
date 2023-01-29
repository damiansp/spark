path = '/path/to/sf-airbnb-clean.parquet'
abb_df = spark.read.parquet(path)
abb_df.select(
    'neighbouhood cleansed',
    'room_type',
    'bedrooms',
    'bathrooms',
    'number_of_reviews',
    'price'
).show(5)
train, test = abb_df.randomSplit([0.8, 0.2], seed=42)
print(f'Training size: {train.count()}, Test: {test.count()}')
