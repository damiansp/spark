from math import log
from pprint import pprint
from random import randint

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import (
    OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler)
from pyspark.spark.ml.linalg import Vector, Vectors
from pyspark.sql import DataFrame
from pyspark.sql.functions import collect_list


# First take on clustering
data_without_header = (spark.read
                       .option('inferSchema', True)
                       .option('header', False)
                       .csv('data/kdd_1999_data'))
column_names = [
    'duration', 'protocol_type', 'service', 'flag', 'src_bytes', 'dst_bytes',
    'land', 'wrong_fragment', 'urgent', 'hot', 'num_failed_logins', 'logged_in',
    'num_compromised', 'root_shell', 'su_attempted', 'num_root',
    'num_file_creations', 'num_shells', 'num_access_files', 'num_outbound_cmds',
    'is_host_login', 'is_guest_login', 'count', 'srv_count', 'serror_rate',
    'srv_serror_rate', 'rerror_rate', 'srv_rerror_rate', 'same_srv_rate',
    'diff_srv_rate', 'srv_diff_host_rate', 'dst_host_count',
    'dst_host_srv_count', 'dst_host_same_srv_rate', 'dst_host_diff_srv_rate',
    'dst_host_same_src_port_rate', 'dst_host_srv_diff_host_rate',
    'dst_host_serror_rate', 'dst_host_srv_serror_rate', 'dst_host_rerror_rate',
    'dst_host_srv_rerror_rate', 'label']
data = data_without_header.toDF(*column_names)
data.select('label')\
    .groupBy('label')\
    .count()\
    .orderBy(col('count').desc())\
    .show(25)

numeric_only = data.dron('protocol_type', 'service', 'flag').cache()
assembler = (VectorAssembler()
             .setInputCols(numeric_only.columns[:-1])
             .setOutputCol('featureVector'))
kmeans = KMeans().setPredictionCol('cluster').setFeaturesCol('featureVector')
pipeline = Pipeline().setStages([assembler, kmeans])
pipeline_model = pipeline.fit(numeric_only)
kmeans_model = pipeline_model.stages[1]
pprint(kmeans_model.clusterCenters())

with_cluster = pipeline_model.transform(numeric_only)
with_cluster.select('cluster', 'label')\
            .groupBy('cluster', 'label')\
            .cont()\
            .orderBy(col('cluster'), col('count').desc())\
            .show(25)


# Choosing k
def clustering_score(data, k):
    numeric_only = data.drop('protocol_type', 'service', 'flag')
    assembler = (VectorAssembler()
                 .setInputCols(numeric_only.columns[:-1])
                 .setOutputCol('featureVector'))
    kmeans = (KMeans()
              .setSeed(randint(100, 100_000))
              .setK(k)
              .setPredictionCol('cluster')
              .setFeaturesCol('featureVector'))
    pipeline = Pipeline().setStages([assembler, kmeans])
    pipeline_model = pipline.fit(numeric_only)
    evaluator = ClusteringEvaluator(predictionCol='cluster',
                                    featuresCol='featureVector')
    preds = pipeline_model.transform(numeric_only)
    score = evaluator.evaluate(preds)
    return score


for k in range(20, 100, 20):
    print(clustering_score(numeric_only, k))


def clustering_score_1(data, k):
    numeric_only = data.drop('protocol_type', 'service', 'flag')
    assembler = (VectorAssembler()
                 .setInputCols(numeric_only.columns[:-1])
                 .setOutputCol('featureVector'))
    kmeans = (KMeans()
              .setSeed(randint(100, 100_000))
              .setK(k)
              .setMaxIter(40)
              .setTol(1.0e-5)
              .setPredictionCol('cluster')
              .setFeaturesCol('featureVector'))
    pipeline = Pipeline().setStages([assembler, kmeans])
    pipeline_model = pipline.fit(numeric_only)
    evaluator = ClusteringEvaluator(predictionCol='cluster',
                                    featuresCol='featureVector')
    preds = pipeline_model.transform(numeric_only)
    score = evaluator.evaluate(preds)
    return score


for k in range(20, 100, 20):
    print(k, clustering_score_1(numeric_only, k))


def clustering_score_2(data, k):
    numeric_only = data.drop('protocol_type', 'service', 'flag')
    assembler = (VectorAssembler()
                 .setInputCols(numeric_only.columns[:-1])
                 .setOutputCol('featureVector'))
    scaler = (StandardScaler()
              .setInputCol('featureVector')
              .setOutputCol('scaledFeatureVector')
              .setWithStd(True).
              setWithMean(False))
    kmeans = (KMeans()
              .setSeed(randint(100, 100_000))
              .setK(k)
              .setMaxIter(40)
              .setTol(1.0e-5)
              .setPredictionCol('cluster')
              .setFeaturesCol('scaledFeatureVector'))
    pipeline = Pipeline().setStages([assembler, scaler, kmeans])
    pipeline_model = pipline.fit(numeric_only)
    evaluator = ClusteringEvaluator(predictionCol='cluster',
                                    featuresCol='featureVector')
    preds = pipeline_model.transform(numeric_only)
    score = evaluator.evaluate(preds)
    return score


for k in range(20, 100, 20):
    print(k, clustering_score_2(numeric_only, k))


def onehot_pipeline(input_col):
    indexer = (StringIndexer()
               .setInputCol(input_col)
               .setOutputCol(f'{input_col}_indexed'))
    encoder = (OneHotEncoder()
               .setInputCol(f'{input_col}_indexed')
               .setOutputCol(f'{input_col}_vec'))
    pipeline = Pipeline().setStages([indexer, encoder])
    return pipeline, f'{input_col}_vec'


def entropy(counts):
    vals = [v for v in vals if v > 0]
    n = sum(vals)
    p = [v / n for v in vals]
    return sum([-p_v * log(p_v) for p_v in p])


cluster_label = pipeline_model.transform(data).select('cluster', 'label')
weighted_cluster_entropy = (cluster_label
                            .groupBy(col('cluster'))
                            .agg(collect_list('label')))
print(sum(weighted_cluster_entropy) / data.count())

# why not just throw in some functions not defined anywhere...?
pipeline_mod = fit_pipeline_4(data, 180)
count_by_cluster_label = (pipeline_mod
                          .transform(data)
                          .select('cluster', 'label')
                          .groupBy('cluster', 'label')
                          .count()
                          .orderBy('cluster', 'label'))
count_by_cluster_label.show()

k_means_mod = pipeline_mod.stages[-1]
centroids = k_means_mod.clusterCenters
clustered = pipeline_mod.transform(data)
threshold = (
    clustered
    .select('cluster', 'scaledFeatureVector')
    .withColumn(
        'dist_values',
        Vectors.squared_distance(centroids(col('cluster')),
                                 col('scaledFeatureVector')))
    .orderBy(col('value').desc())
    .last)

# and text has remainder of chapter in Scala... nice!
