from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName('Parquet').getOrCreate()
sc = spark.sparkContext


# Loading
people_df = spark.read.json('people.json')
people_df.write.parquet('people.parquet')
people_df = spark.read.parquet('people.parquet')
people_df.createOrReplaceTempView('people')
teens = spark.sql('SELECT name FROM people WHERE age >= 13 and age < 20')
teens.show()


# Schema Merging
squares_df = spark.createdDataFrame(
    sc.parallelize(range(1, 6).map(lambda i: Row(linear=i, quadratic=i ** 2))))
squares_df.write.parquet('data/test_table/key=1')
cubes_df = spark.createdDataFrame(
    sc.parallelize(range(6, 11).map(lambda i: Row(linear=i, cubic=i ** 3))))
cubes_df.write.parquet('data/test_table/key=2')
merged = spark.read.option('mergeSchema', 'true').parquet('data/test_table')
merged.printSchema()  # linear, quadratic, cubic, key


# Columnar Encryption
# Set hadoop configuration properties, e.g. using configuration properties of
# the Spark job:
# --conf spark.hadoop.parquet.encryption.kms.client.class=\
#           "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS"\
# --conf spark.hadoop.parquet.encryption.key.list=\
#           "keyA:AAECAwQFBgcICQoLDA0ODw== ,  keyB:AAECAAECAAECAAECAAECAA=="\
# --conf spark.hadoop.parquet.crypto.factory.class=\
#           "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory"

# Write encrypted dataframe files.
# Column "square" will be protected with master key "keyA".
# Parquet file footers will be protected with master key "keyB"
(squares_df
 .write
 .option('parquet.encryption.column.key', 'keyA:square')
 .option('parquet.encryption.footer.key', 'keyB')
 .parquet('my.parquet.encrypted'))
df2 = spark.read.parquet('my.parquet.encrypted')


# Data Source Option
