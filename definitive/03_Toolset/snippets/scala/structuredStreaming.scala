import org.apache.spark.sql.functions.{col, column, desc, window}


// reasonable setting for local-mode (vs default of 200)                          
spark.conf.set("spark.sql.shuffle.partitions", "5")

val dataPath = "/data/retail-data/by-day/*.csv"
val staticDF = spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(dataPath)
staticDF.createOrReplaceTempView("retail_data")
val staticSchema = staticDF.schema

staticDF.selectExpr("CustomerId", "(UnitPrice * Quantity) AS total_cost", "InvoiceDate" )
  .groupBy(col("CustomerId"), window(col("InvoiceDate", "1 day")))
  .sum("total_cost")
  .show(5)

val streamingDF = spark.readStream
  .schema(staticSchema)
  .option("maxFilesPerTrigger", 1)
  .format("csv")
  .option("header", "true")
  .load(dataPath)
println(streamingDF.isStreaming) // true

val purchaseByCustomerPerHours = streamingDF
  .selectExpr("CustomerId", "(UnitPrice * Quantity) AS total_cost", "InvoiceDate" )
  .groupBy($"CustomerId", window($"InvoiceDate", "1 day"))
  .sum("total_cost")