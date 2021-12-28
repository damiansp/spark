from pprint import pprint

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler


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
pipline_model = pipline.fit(numeric_only)
kmeans_model = pipline_model.stages[1]
pprint(kmeans_model.clusterCenters())
