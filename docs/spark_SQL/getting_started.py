from pyspark.sql import SparkSession


DATA = '../../data'
spark = SparkSession.builder.appName('SQL').getOrCreate()

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
