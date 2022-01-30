import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max


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

  println(spark.sql("SELECT MAX(count) FROM flightData").take(1))
  println(data.select(max("count")).take(1))

  val maxSql = spark.sql("""
    SELECT DEST_COUNTRY_NAME, SUM(count) AS dest_total
    FROM flightData
    GROUP BY DEST_COUNTRY_NAME
    ORDER BY SUM(count) DESC
    LIMIT 5""")
  maxSql.show()
}
