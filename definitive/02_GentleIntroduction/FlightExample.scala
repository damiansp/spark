import org.apachespark.sql.SparkSession


object FlightExample {
  val DATA = "../../data"

  spark = SparkSession.builder.appName("flights").getOrCreate()
  val data = spark
    .read
    .option("inferScehma", "true")
    .option("header", "true")
    .csv(s"$DATA/flight-data/csv/2015-summary.csv")

  // DataFrames and SQL
  data.createOrReplaceTempView("flightData")

  val sqlWay = spark.sql("""
    SELECT DEST_COUNTRY_NAME, COUNT(1)
    FROM flightData
    GROUP BY DEST_COUNTRY_NAME""")
  val dfWay = data.groupBy('DEST_COUNTRY_NAME).count()
  println(sqlWay.explain)
  println(dfWay.explain)
}
