from psyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, split, min, max
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


user_artist_df = (raw_user_artist_data
                  .withColumn('user', split(raw_user_artist_data['value'], ' ')
                              .getItem(0)
                              .cast(IntegerType()))
                  .withColumn('artist', split(raw_user_artist_data['value'], ' ')
                              .getItem(1)
                              .cast(IntegerType()))
                  .withColumn('count', split(raw_user_artist_data['value'], ' ')
                              .getItem(2)
                              .cast(IntegerType()))
                  .drop('value'))
user_artist_df.select(
    [min('user'), max('user'), min('artist'), max('artist')]
).show()

artist_by_id = (raw_artist_data
                .withColumn('id', split(col('value'), '\s+', 2)
                            .getItem(0)
                            .cast(IntegerType()))
                .withColumn('name', split(col('value'), '\s+', 2)
                            .getItem(1)
                            .cast(StringType()))
                .drop('value'))
artist_by_id.show(5)

artist_alias = (raw_artist_alias
                .withColumn('artist', split(col('value'), '\s+')
                            .getItem(0)
                            .cast(IntegerType()))
                .withColumn('alias', split(col('value'), '\s+')
                            .getItem(1)
                            .cast(StringType()))
                .drop('value'))
artist_alias.show(5)

# Example lookup
artist_by_id.filter(artist_by_id.id.isin(1092764, 1000311)).show()



# Building an Initial Model
