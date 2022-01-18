import org.apachespark.sql.SparkSession


object FlightExample {
  val DATA = "../../data"

  spark = SparkSession.builder.appName("flights").getOrCreate()
  val flightData2015 = spark
                        .read
                        .option("inferScehma", "true")
                        .option("header", "true")
                        .csv(s"$DATA/flight-data/csv/2015-summary.csv"))

}
