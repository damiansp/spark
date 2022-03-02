from pyspark.sqpl.types import ByteType


df = spark.range(500).toDF('number')
df.select(df['number'] + 10)  # executed in Spark, not python


# Rows
spark.range(2).collect()  # Row object


# Spark types
b = ByteType()
