from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
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


if __name__ == '__main__':
    main()
