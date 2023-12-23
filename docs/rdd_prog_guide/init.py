from pyspark import SparkConf, SparkContext


# Typically set master in spark-submit
conf = SparkConf().setAppName('MyApp')#.setMaster('local')
sc = SparkContext(conf=conf)

data = [1, 2, 3, 4, 5]
dist_data = sc.parallelize(data)

dist_file = sc.textFile('data.txt')
