import org.apache.spark.sql.functions.{col, column}
import org.apache.spark.sql.types.{
  LongType, Metadata, StringType, StructField, StructType}


/* Schemata */
val path = "/data/flight-data/json/2015-summary.json"
val df = spark.read.format("json").load(path) // or

df.printSchema()

spark.read.format("json").load(path).schema()

val mySchema = StructType(
  Array(
    StructField("DEST_COUNTRY_NAME", StringType, true),
    StructField("ORIGIN_COUNTRY_NAME", StringType, true),
    StructField(
      "count", LongType, false, Metadata.fromJson("{\"hello\": \"world\"}")))
val df2 = spark.read.format("json")schema(mySchema).load(path)


/* Columns and Expressions */
// all equivalent:
col("myCol")
column("myCol")
$"myCol"
'myCol
