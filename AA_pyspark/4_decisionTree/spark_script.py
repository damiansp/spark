from pprint import pprint

import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.classification import (DecisionTreeClassifier,
                                       RandomForestClassifer)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, VectorIndexer
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql.import DataFrame as DF
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType


DATA = '../../data'


data_sans_header = (spark.read.option('inferSchem', True)
                    .option('header', False)
                    .csv(f'{DATA}/covtype.data'))
data_sans_header.printSchema()

col_names = (
    ['Elevation', 'Aspect', 'Slope', 'Horizontal_Distance_To_Hydrology',
     'Vertical_Distane_To_Hydrology', 'Horizontal_Distance_To_Roadways',
     'Hillshade_9am', 'Hillshade_Noon', 'Hillshade_3pm',
     'Horizontal_Distance_To_Fire_Points']
    + [f'Wilderness_Area_{i}' for i in range(4)]
    + [f'Soil_Type_{i}' for i in range(40)]
    + ['Cover_Type'])
data = (data_sans_header.toDF(*col_names)
        .withColumn('Cover_Type', col('Cover_Type').cast(DoubleType())))
print(data.head())


# First Tree
train_data, test_data = data.randomSplit([0.9, 0.1])
train_data.cache()
test_data.cache()

input_cols = col_names[:-1]
vector_assembler = VectorAssembler(inputCols=input_cols,
                                   outputCol='featureVector')
assembled_train_data = vector_assembler.transform(train_data)
assembled_train_data.select('featureVector').show(truncate=False)

classifier = DecisionTreeClassifier(seed=123, l
                                    abelCol='Cover_Type',
                                    featuresCol='featureVector',
                                    predictionCol='prediction')
model = classifier.fit(assembled_train_data)
print(model.toDebugString)

importance = pd.DataFrame(
    model.featureImportances.toArray(), index=inputCols, columns=['importance']
).sort_values(by='importance', ascending=False)
print(importance)

predictions = model.transform(assembled_train_data)
predictions.select('Cover_Type', 'prediction', 'probability')\
           .show(10, truncate=False)

evaluator = MulticlassClassificationEvaluator(labelCol='Cover_Type',
                                              predictionCol='prediction')
print('Acc:', evaluator.setMetricName('accuracy').evaluate(predictions))
print('F1:', evaluator.setMetricName('f1').evaluate(predictions))

confusion_matrix = (predictions.groupBy('Cover_Type')
                    .pivot('prediction', range(1, 8))
                    .count()
                    .na.fill(0.)
                    .orderBy('CoverType'))
confusion_matrix.show()

total = data.count()


def class_probabilities(data):
    out = (data.groupBy('Cover_Type')
           .count()
           .orderBy('Cover_Type')
           .select(col('count').cast(DoubleType()))
           .withColumn('count_proportion', col('count') / total)
           .select('count_proportion').collect())
    return out

train_prior_probs = class_probabilities(train_data)
test_prior_probs = class_probabilities(test_data)
print(train_prior_probs)

train_prior_probs = [p[0] for p in train_prior_probs]
test_prior_probs = [p[0] for p in test_prior_probs]
baseline_acc = sum(
    [train_p * cv_p
     for train_p, cv_p in zip(train_prior_probs, test_prior_probs)])
print('Baseline:', baseline_acc)


# Tuning
assembler = VectorAssembler(inputCols=input_cols, outputCol='featureVector')
classifier = DecisionTreeClassifier(seed=111,
                                    labelCol='Cover_Type',
                                    featuresCol='featureVector',
                                    predictionCol='prediction')
pipeline = Pipeline(stages=[assembler, classifier])

param_grid = (ParamGridBuilder()
              .addGrid(classifier.impurity, ['gini', 'entropy'])
              .addGrid(classifier.maxDepth, [1, 20])
              .addGrid(classifier.maxBins, [40, 300])
              .addGreid(classifier.minInfoGain, [0, 0.05])
              .build())
multiclass_eval = (MulticlassClassificationEvaluator()
                   .setLabelCol('Cover_Type')
                   .setPredictionsCol('prediction')
                   .setMetricName('accuracy'))
validator = TrainValidationSplit(seed=222,
                                 estimator=pipeline,
                                 evaluator=multiclass_eval,
                                 estimatorParamMaps=paramGrid,
                                 trainRatio=0.8)
validator_model = validator.fit(train_data)

best_model = validator_model.bestModel
pprint(best_model.stages[1].extractParamMap())

metrics = validator_model.validationMetrics
params = validator_model.getEstimatorParamMaps()
metrics_and_params = list(zip(metrics, params))
metrics_and_params.sort(key=lambda x: x[0], reverse=True)
print(metrics_and_params)

metrics.sort(reverse=True)
print(metrics[0]) # highest accuracy
print(multiclass_eval.evaluate(best_model.transform(test_data))) # on test set


def one_cold(data):
    '''reverse one-hot encoding'''
    wilderness_cols = ['Wilderness_Area_' + str(i) for i in range(4)]
    wilderness_assembler = (VectorAssembler()
                            .setInputCols(wilderness_colls)
                            .setOutputCol('wilderness'))
    unhot_udf = udf(lambda v: v.toArray().tolist().index(1))
    with_wilderness = (wilderness_assembler
                       .transform(data)
                       .drop(*wilderness_cols)
                       .withColumn('wilderness', unhot_udf(col('wilderness'))))
    soil_cols = ['Soil_Type_' + str(i) for i in range(40)]
    soil_assembler = (
        VectorAssembler().setInputCols(soil_cols).setOutputCol('soil'))
    with_soil = (soil_assembler
                 .transform(with_wilderness)
                 .drop(*soil_cols)
                 .withColumn('soil', unhot_udf(col('soil'))))
    return with_soil


onecold_train_data = one_cold(train_data)
cols = onecold_train_data.columns
input_cols = [c for c in cols if c != 'Cover_Type']
assembler = (VectorAssembler()
             .setInputCols(input_cols)
             .setOutputCol('featureVector'))
indexer = (VectorIndexer()
           .setMaxCategories(40)
           .setInputCol('featureVector')
           .setOutpuCol('indexedVector'))
classifier = (DecisionTreeClassifier()
              .setLabelCol('Cover_Type')
              .setFeaturesCol('indexedVector')
              .setPredictionCol('prediction'))
pipeline = Pipeline().setStages([assembler, indexer, classifier])



# Forests
classifer = RandomForestClassifier(seed=321,
                                   labelCol='Cover_Type',
                                   featuresCol='indexedVector',
                                   predictionCol='prediction')
forest_model = best_model.stages[1]
feature_importance_list = list(
    zip(input_cols, forest_model.featureImportances.toArray()))
feature_importance_list.sort(key=lambda x: x[1], reverse=True)
pprint(feature_importance_list)
