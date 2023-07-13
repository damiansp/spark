from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator as MCEval
from pyspark.ml.feature import Normalizer, StringIndexer, VectorAssembler


ITERS = 10


def main():
    df = load_and_prep_data()  # omitted
    df = encode_categoricals(df)
    df = cast(df)
    df = assemble_features(df)
    df = normalize(df)
    train_ds, test_ds = split_train_test(df)
    lr = LogisticRegression(
        featuresCol='features_norm',
        labelCol='label',
        maxIter=ITERS,
        regParam=0.3)
    # , elasticNetParam=0.8)
    preds = mod
    pipeline = Pipeline(stages=[lr])
    mod = pipeline.fit(train_ds)
    fitted = mod.transform(train_ds)
    preds = mod.transform(test_ds)
    evaluator = (
        MCEval()
        .setMetricName('accuracy')
        .setPredictionCol('pred')
        .setLabelCol('label'))
    print(evaluator.evaluate(preds))
    

def encode_categorical(df):
    indexer = StringIndexer(inputCol='class', outputCol='label')
    indexed = indexer.fit(df).transform(df)
    indexed.show()
    indexed.select('label').distinct().show()
    return indexed


def cast(df):
    numeric_cols = [
        'age', 'bp', 'sg', 'bgr', 'bu', 'sc', 'sod', 'pot', 'hemo', 'pcv',
        'wbcc', 'rbcc']
    for col in numeric_cols:
        df = df.withColoumn(col, df[col].cast(DoubleType()))
    return df


def assemble_features(df):
    assembler = VectorAssembler(
        inputCols=['list', 'of', 'features'], outputCol='features')
    vectorized = assembler.transform(df)
    vectorized.show()
    return vectorized


def normalize(df):
    normalizer = Normalizer(
        inputCol='features', outputCol='features_norm', p=1.)
    l1_norm_data = normalizer.transform(df)
    l1_norm_data.show()
    return l1_norm_data


def split_train_test(df):
    train, test = df.randomSplit([0.8, 0.2])
    return train, test
