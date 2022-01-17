val df = spark
  .read
  .option("samplingRation", 0.001)
  .option("header", true)
  .csv("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

// Using schema
val fireSchema = StructType(
  Array(
    StructField("CallNumber", IntegerType(), true),
    StructField("UnitId", StringType(), true),
    StructField("IncidentNumber", IntegerType(), true),
    StructField("CallType", StringType(), true),
    StructField("CallDate", StringType(), true),
    StructField("WatchDate", StringType(), true),
    StructField("CallFinalDisposition", StringType(), true),
    StructField("AvailableDtTm", StringType(), true),
    StructField("Address", StringType(), true),
    StructField("City", StringType(), true),
    StructField("Zipcode", IntegerType(), true),
    StructField("Battalion" StringType(), true),
    StructField("StationArea", StringType(), true),
    StructField("Box", StringType(), true),
    StructField("OriginalPriority", StringType(), true),
    StructField("Priority", StringType(), true),
    StructField("FinalPriority", StringType(), true),
    StructField("ALSUnit", BooleanType(), true),
    StructField("CallTypeGroup", StringType(), true),
    StructField("NumAlarms", IntegerType(), true),
    StructField("UnitType", StringType(), true),
    StructField("UnitSequenceInCallDispatch", IntegerType(), true),
    StructField("FirePreventionDistrict", StringType(), true),
    StructField("SupervisorDistrict", StringType(), true),
    StructField("Neighborhood", StringType(), true),
    StructField("Location", StringType(), true),
    StructField("RowID", StringType(), true),
    StructField("Delay", FloatType(), true)))
val sfFireFile = "/databricks-datasets/learning-sark-v2/sf-fire/sf-fire-calls.csv"
val fireDF = spark.read.schema(fireSchema).option("header", "true").csv(sfFireFile)

val parquetPath = "/databricks-datasets/learning-sark-v2/sf-fire/sf-fire-calls.parquet"
fireDF.write.format("parquet").save(parquetPath) // or as table
val parquetTable = "sfFires"
fireDF.write.format("parquet").saveAsTable(parquetTable)


// Transformations/Actions
val fewFireDF = fireDF
  .select("IncidentNumber", "AvailableDtTm", "CallType")
  .where(col("CallType" != "Medical Incident"))
fewFireDF.show(5, false)