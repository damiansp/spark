from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('App Name')
sc   = SparkContext(conf = conf)
