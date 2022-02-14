import org.apache.spark.sql.functions.date_format

staticDF.printSchema()

val preppedDF = staticDF
  .na.fill(0)
  .withColumn("day_of_week", date_format($"InvoiceDate", "EEEE"))
  .coalesce(5)
val trainDF = preppedDF.where("InvoiceDate < '2011-07-01'")
val testDF  = preppedDF.where("InvoiceDate >= '2011-07-01'")

println(trainDF.count())
println(testDF.count())
