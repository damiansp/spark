from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('SimpleApp').getOrCreate()
infile = './test.txt'
data = spark.read.text(infile).cache()

n_a = data.filter(data.value.contains('a')).count()
n_b = data.filter(data.value.contains('b')).count()
n_c = data.filter(data.value.contains('c')).count()
print(f'Lines with:\n  a: {n_a}\n  b: {n_b}\n  c: {n_c}')
spark.stop()
