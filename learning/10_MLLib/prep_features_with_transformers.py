from pyspark.ml.feature import VectorAssembler


vec_assembler = VectorAssembler(inputCols=['bedrooms'], outputCol='features')
vec_train_df = vec_assembler.transform(train_df)
vec_train_df.select('bedrooms', 'features', 'price').show(10)
