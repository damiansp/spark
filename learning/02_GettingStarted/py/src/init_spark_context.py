from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster('local').setAppName('Init')
sc = SparkContext(conf=conf)

print('sc:', sc)
