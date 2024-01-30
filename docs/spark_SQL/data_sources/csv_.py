from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('CSV').getOrCreate()
sc = spark.sparkContext

path = './people.csv'
spark.read.csv(path).show()
spark.read.option('delimiter', ';').csv(path).show()
spark.read.option('delimiter', ';').option('header', True).csv(path).show()
df = spark.read.options(delimiter=';', header=True).csv(path)
df.show()


df.write.csv('output')

# Read all files in a directory
#folder = 'my/output'
#df = spark.read.csv(folder)
