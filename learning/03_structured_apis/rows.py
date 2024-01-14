from pyspark.sql import Row, SparkSession


spark = SparkSession.builder.appName('Rows').getOrCreate()
row = Row('Matei Zaharia', 'CA')
print(row[0])
rows = [row, Row('Reynold Xin', 'CA')]
df = spark.createDataFrame(rows, ['author', 'state'])
df.show()
