from pyspark import SparkConf, SparkContext


conf = SparkConf().setAppName('Text')
sc = SparkContext(conf=conf)

lines = sc.textFile('../../README.md')
