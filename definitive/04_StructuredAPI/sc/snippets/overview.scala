import org.apache.spark.sql.types._


val df = spark.range(500).toDF("number")
df.select(df.col("number") + 10) // executed in Spark, not Scala


// Rows
spark.range(2).toDF().collect() // Row object


// Spark Types
val b = ByteType // Scala type
