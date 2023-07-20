from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.ml.classification import (
    LogisticRegression, LogisticRegressionModel)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator as BCEval,
    MulticlassClassificationEvaluator as MCEval)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


spark = SparkSession.builder.appName('Logistic').getOrCreate()


def main():
    df = load_data()
    train_ds, test_ds = prep_data(df)
    log_reg = LogisticRegression(featuresCol='features', labelCol='label')
    mod = log_reg.fit(train_ds)
    intercept = mod.intercept
    coefs = mod.coefficients
    print(f'Intercept: {intercept:.3f}')
    print('Coefs:', coefs)
    evaluate_model(mod, test_ds)
    mod.save('test_mod')
    mod = LogisticRegressionModel.load('test_mod')
    

def load_data():
    url = (
        'https://raw.githubusercontent.com/pkmklong/'
        'Breast-Cancer-Wisconsin-Diagnostic-Dataset/master/data.csv')
    spark.sparkContext.addFile(url)
    df = spark.read.csv(
        SparkFiles.get('data.csv'), header=True, inferSchema=True)
    df.show(5)
    return df


def prep_data(df):
    # rename cols
    cols = ['id', 'diagnosis'] + [f'feature_{i}' for i in range(1, 32)]
    data = df.toDF(*cols)
    # Map repsonse 'M' (malignant) -> 1, 'B' (benign) -> 0
    data = (
        data
        .withColumn('label', (data.diagnosis == 'M').cast('integer'))
        .drop('diagnosis'))
    feature_cols = [f'feature_{i}' for i in range(1, 25)]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    data = assembler.transform(data)
    train, test = data.randomSplit([0.8, 0.2], seed=42)
    return train, test


def evaluate_model(mod, test_ds):
    preds = mod.transform(test_ds)
    evaluator = BCEval(rawPredictionCol='rawPrediction', labelCol='label')
    auc = evaluator.evaluate(preds)
    multiclass_eval = MCEval(labelCol='label', predictionCol='prediction')
    acc = multiclass_eval.evaluate(
        preds, {multiclass_eval.metricName: 'accuracy'})
    prec = multiclass_eval.evaluate(
        preds, {multiclass_eval.metricName: 'weightedPrecision'})
    rec = multiclass_eval.evaluate(
        preds, {multiclass_eval.metricName: 'weightedRecall'})
    print(f'AUC:  {auc:.4f}')
    print(f'Acc:  {acc:.4f}')
    print(f'Prec: {prec:.4f}')
    print(f'Rec:  {rec:.4f}')


if __name__ == '__main__':
    main()
