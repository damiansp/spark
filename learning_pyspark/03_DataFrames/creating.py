from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType


conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession(sparkContext=sc).builder.appName('create').getOrCreate()

json_str1 = '{"id": "123", "name": "Katie", "age": 19, "eyes": "brown"}'
json_str2 = '{"id": "234", "name": "Michael", "age": 22, "eyes": "green"}'
json_str3 = '{"id": "345", "name": "Simone", "age": 23, "eyes": "blue"}'
json_rdd = sc.parallelize((json_str1, json_str2, json_str3))
df = spark.read.json(json_rdd)
df.show()
df.printSchema()

# create a temp table
df.createOrReplaceTempView('swimmers')
print(spark.sql('SELECT * FROM swimmers').collect())

csv_rdd = sc.parallelize([
    (123, 'Katie', 19, 'brown'),
    (234, 'Michael', 22, 'green'),
    (345, 'Simone', 23, 'blue')])
schema = StructType([
    StructField('id', LongType(), True),
    StructField('name', StringType(), True),
    StructField('age', LongType(), True),
    StructField('eyes', StringType(), True)])
swimmers = spark.createDataFrame(csv_rdd, schema)
swimmers.createOrReplaceTempView('swimmers')
swimmers.printSchema()
print(swimmers.count())
swimmers.select('id', 'age').filter('age >= 22').show()
swimmers.select('name', 'eyes').filter("eyes LIKE 'b%'").show()

spark.sql('SELECT COUNT(1) FROM swimmers').show()
spark.sql('SELECT id, age FROM swimmers WHERE age >= 22').show()
spark.sql("SELECT name, eyes FROM swimmers WHERE eyes LIKE 'b%'").show()
