from pyspark import SparkConf, SparkContext


# Typically set master in spark-submit
conf = SparkConf().setAppName('MyApp')#.setMaster('local')
sc = SparkContext(conf=conf)
