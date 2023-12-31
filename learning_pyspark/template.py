from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession(sparkContext=sc).builder.appName('').getOrCreate()
