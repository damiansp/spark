import org.apache.spark.sql.types._


val schema = StructType(
  Array(
    StructField("author", StringType, false),  // false = Nullable?
    StructField("title", StringType, false), 
    StructField("pages", IntegerType, false)))

// With DDL
val schema2 = "author STRING, title STRING, pages INT"