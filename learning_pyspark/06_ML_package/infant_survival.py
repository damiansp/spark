from pyspark.ml import Pipeline
import pyspark.ml.classification as cl
import pyspark.ml.evaluation as ev
import pyspark.ml.feature as ft
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


labels = [
    ('INFANT_ALIVE_AT_REPORT', IntegerType()),
    ('BIRTH_PLACE', StringType()),
    ('MOTHER_AGE_YEARS', IntegerType()),
    ('FATHER_COMBINED_AGE', IntegerType()),
    ('CIG_BEFORE', IntegerType()),
    ('CIG_1_TRI', IntegerType()),
    ('CIG_2_TRI', IntegerType()),
    ('CIG_3_TRI', IntegerType()),
    ('MOTHER_HEIGHT_IN', IntegerType()),
    ('MOTHER_PRE_WEIGHT', IntegerType()),
    ('MOTHER_DELIVERY_WEIGHT', IntegerType()),
    ('MOTHER_WEIGHT_GAIN', IntegerType()),
    ('DIABETES_PRE', IntegerType()),
    ('DIABETES_GEST', IntegerType()),
    ('HYP_TENS_PRE', IntegerType()),
    ('HYP_TENS_GEST', IntegerType()),
    ('PREV_BIRTH_PRETERM', IntegerType())]
schema = StructType([StructField(e[0], e[1], False) for e in labels])
births = spark.read.csv(
    f'{DATA}/births_transformed.csv.gz', header=True, schema=schema)
births = (
    births
    .withColumn('BIRTH_PLACE_INT', births['BIRTHPLACE'].cast(IntegerType())))
encoder = ft.OneHotEncoder(
    inputCol='BIRTH_PLACE_INT', outputCol='BIRTH_PLACE_VEC')
feature_creator = ft.VectorAssembler(
    inputCols=[col[0] for col in labels[2:]] + [encoder.getOutputCol()],
    outputCols'features')
logistic = cl.LogisticRegression(
    maxIter=10, regParam=0.01, labelCol='INFANT_ALIVE_AT_REPORT')
pipeline = Pipeline(stages=[encoder, feature_creator, logistic])
train, test = births.randomSplit([0.7, 0.3], seed=666)
mod = pipeline.fit(train)
test_mod = mod.transform(test)

evaluator = ev.BinaryClassificationEvaluator(
    rawPredictionCol='probability', labelCol='INFANT_ALIVE_AT_REPORT')
print(
    'AUC:',
    evaluator.evaluate(test_mod, {evaluator.metricName: 'areaUnderROC'}))
print(
    'AUPR:',
    evaluator.evaluate(test_mod, {evaluator.metricName: 'areaUnderROC'}))
