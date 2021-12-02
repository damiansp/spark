from psyspark.ml.recommendation import ALS
from pyspark.sql.functions import broadcast, col, min, max, split, when
from pyspark.sql.types import IntegerType, StringType


DATA = '../../data/audioscrobbler'


raw_user_artist_data = spark.read.text(f'{DATA}/user_artist_data.txt')
raw_user_artist_data.show(5)

raw_artist_data = spark.read.text(f'{DATA}/artist_data.txt')
raw_artist_data.show(5)

raw_artist_alias = spark.read.text(f'{DATA}/artist_alias.txt')
raw_artist_alias.show(5)


#als = ALS(maxIter=5,
#          regParam=0.01,
#          userCol='user',
#          itemCol='artist',
#          ratingCol='count')
#mod = als.fit(train)
#predictions = model.transform(test)


user_artist_df = (
    raw_user_artist_data
    .withColumn(
        'user',
        split(raw_user_artist_data['value'], ' ').getItem(0).cast(IntegerType())
    ).withColumn(
        'artist',
        split(raw_user_artist_data['value'], ' ').getItem(1).cast(IntegerType())
    ).withColumn(
        'count',
        split(raw_user_artist_data['value'], ' ').getItem(2).cast(IntegerType())
    ).drop('value'))
user_artist_df.select(
    [min('user'), max('user'), min('artist'), max('artist')]
).show()

artist_by_id = (
    raw_artist_data
    .withColumn(
        'id',
        split(col('value'), '\s+', 2).getItem(0).cast(IntegerType()))
    .withColumn(
        'name',
        split(col('value'), '\s+', 2).getItem(1).cast(StringType()))
    .drop('value'))
artist_by_id.show(5)
artist_alias = (
    raw_artist_alias
    .withColumn(
        'artist',
        split(col('value'), '\s+').getItem(0).cast(IntegerType()))
    .withColumn(
        'alias',
        split(col('value'), '\s+').getItem(1).cast(StringType()))
    .drop('value'))
artist_alias.show(5)

# Example lookup
artist_by_id.filter(artist_by_id.id.isin(1092764, 1000311)).show()



# Building an Initial Model
train_data = (
    user_artist_df
    .join(broadcast(artist_alias), 'artist', how='left')
    .withColumn(
        'artist',
        when(col('alias').isNull(), col('artist')).otherwsie(col('alias')))
    .withColumn('artists', col('artist').cast(IntegerType()))
    .drop('alias'))
train_data.cache()

model = ALS(
    rank=10,
    seed=1107,
    maxIter=5,
    regParam=0.1,
    implicitPrefs=True,
    alpha=1.,
    userCol='user',
    itemCol='artist',
    ratingCol='count'
).fit(train_data)
model.userFactors.show(1, truncate=False)



# Spot check
user_id = 2093760
existing_artist_ids = (
    train_data.filter(train_data.user == user_id).select('artist').collect())
existing_artist_ids = [i[0] for i in existing_artist_ids]
artist_by_id.filter(col('id').isin(exisiting_artist_ids)).show()

user_subset = train_data.select('user').weher(col('user') == user_id).distinct()
top_predictions = model.recommendForUserSubset(user_subset, 5)
top_predictions.show()

top_predictions_pandas = top_predictions.toPandas()
print(top_predictions_pandas)

recommended_artist_ids = [i[0]
                          for i in top_predictions_pandas.recommendations[0]]
artist_by_id.filter(col('id').isin(recommended_artist_ids)).show()



# Hyperparam tuning
