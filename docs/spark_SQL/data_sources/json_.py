from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('JSON').getOrCreate()
sc = spark.sparkContext

path = 'people.json'
people_df = spark.read.json(path)
people_df.printSchema()
people_df.createOrReplaceTempView('people')
girls = spark.sql("SELECT name FROM people WHERE name > 'E'")
girls.show()

jstr = [
    '{"name": "Yin", "address": {"city": "Columbus", "state": "OH"}}',
    '{"name": "Yang", "address": {"city": "Boise", "state": "ID"}}']
more_rdd = sc.parallelize(jstr)
more = spark.read.json(more_rdd)
more.show()
