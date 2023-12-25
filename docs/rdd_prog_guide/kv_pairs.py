from pyspark import SparkConf, SparkContext


conf = SparkConf().setAppName('MyApp')
sc = SparkContext(conf=conf)

lines = sc.textFile('data.txt')
pairs = lines.map(lambda s: (s, 1))
counts = pairs.reduceByKey(lambda a, b: a + b)
