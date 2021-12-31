from pprint import pprint
from random import randint

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame


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
    print(clustering_score_1(numeric_only, k))
