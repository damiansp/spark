from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql.functions import col, date_format


prepped_df = (
    static_df
    .na.fill(0)
    .withColumn('day_of_week', date_format(col('InvoiceDate'), 'EEEE'))
    .coalesce(5))

train_df = prepped_df.where("InvoiceDate < '2011-07-01'")
test_df  = prepped_df.where("InvoiceDate >= '2011-07-01'")
print(trainDF.count())
print(trainDF.count())

indexer = (StringIndexer()
           .setInputCol('day_of_week')
           .setOutputCol('day_of_week_index'))
encoder = (OneHotEncoder()
           .setInputCol('day_of_week_index')
           .setOutputCol('day_of_week_encoded'))
vec_assembler = (VectorAssembler()
                 .setInputCols(['UnitPrice', 'Quantity', 'day_of_week_encoded'])
                 .setOutputCol('features'))
transformation_pipeline = (Pipeline()
                           .setStages([indexer, encoder, vecAssembler]))
fitted_pipeline = transformation_pipeline.fit(train_df)
transformed_training = fitted_pipeline.transform(train_df)
transformed_training.cache()

kmeans = KMeans().setK(20).setSeed(1L)
km_model = kmeans.fit(transformed_training)
km_model.computeCost(transformed_training)

transformed_test = fitted_pipeline.transform(test_df)
km_model.computCost(transformed_test)
