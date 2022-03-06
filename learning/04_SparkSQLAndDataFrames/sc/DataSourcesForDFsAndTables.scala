val baseDir = "/databricks-dataset/learning-spark-v2/flights/summary-data/"
val file = baseDir * "parquet/2010-summary.parquet"
val df = spark.read.format("parquet").load(file)
val df2 = spark.read.load(file) // .parquet is default
val df3 = spark
  .read.format("csv")
  .option("inferScema", "true")
  .option("header", "true")
  .option("mode", "PERMISSIVE")
  .load(baseDir * "csv/*")
val df4 = spark.read.format("json").load(baseDir * "json/*")