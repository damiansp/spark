from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


DATA = '../../data'

spark = SparkSession.builder.appName('SQL').getOrCreate()
sc = spark.sparkContext


df = spark.read.json(f'{DATA}/people.jsonl')
df.show()
df.printSchema()
df.select('name').show()
df.select(df['name'], df['age'] + 1).show()
df.filter(df['age'] >= 21).show()
df.groupBy('age').count().show()


# Make temp table
df.createOrReplaceTempView('people')
sql_df = spark.sql('SELECT * FROM people')
sql_df.show()


# Infer schma using reflection
lines = sc.textFile(f'{DATA}/people.txt')
parts = lines.map(lambda l: l.split(','))
people = parts.map(lambda p: Row(name=p[1], age=int(p[0])))

schema_people = spark.createDataFrame(people)
schema_people.createOrReplaceTempView('pople')
teens = spark.sql('SELECT name FROM people WHERE age >= 13 AND age < 20')
teen_names = teens.rdd.map(lambda p: f'Name: {p.name}').collect()
for name in teen_names:
    print(name)

# Specify Schema
schema_headers = ['name', 'age']
fields = [StructField(f, StringType(), True) for f in schema_headers]
schema = StructType(fields)
people = spark.createDataFrame(people, schema)
people.createOrReplaceView('people')
res = spark.sql('SELECT name FROM people')
res.show()
