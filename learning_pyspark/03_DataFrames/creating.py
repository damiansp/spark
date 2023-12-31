from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession(sparkContext=sc).builder.appName('create').getOrCreate()

json_str1 = '{"id": "123", "name": "Katie", "age": 19, "eyes": "brown"}'
json_str2 = '{"id": "234", "name": "Michael", "age": 22, "eyes": "green"}'
json_str3 = '{"id": "345", "name": "Simone", "age": 23, "eyes": "blue"}'
json_rdd = sc.parallelize((json_str1, json_str2, json_str3))
df = spark.read.json(json_rdd)

# create a temp table
df.createOrReplaceTempView('swimmers')
