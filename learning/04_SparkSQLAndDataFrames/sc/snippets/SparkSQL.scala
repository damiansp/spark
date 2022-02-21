import org.apache.spark.sql.SparkSession


val spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
val csvFile = "/databricks-dataset/learning-spakr-v2/flights/departuredelays.csv"
val schema = (
  "date STRING, delay INT, distance INT, origin STRING, destination STRING")
val df = spark.read.format("csv")
  .option("inferSchema", "true")
  .option("header", "true")
  .load(csvFile)
df.createOrReplaceTempView("flightDelays")