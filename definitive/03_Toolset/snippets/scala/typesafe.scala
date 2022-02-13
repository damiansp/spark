case class Flight(DEST_COUNRTY_NAME: String, ORIGIN_COUNTRY_NAME: String, count, BigInt)

val flightsDF = spark.read.parquet("data/flight-data/parquet/2010-summary.parquet/")
val flights = flightsDF.as[Flight]

flights
  .filter(row => row.ORIGIN_COUNTRY_NAME != "Canada")
  .map(row => row) // ???
  .take(5)
flights
  .take(5)
  .filter(row => row.ORIGIN_COUNTRY_NAME != "Canada")
  .map(r => Flight(r.DEST_COUNRTY_NAME, r.ORIGIN_COUNTRY_NAME, r.count + 5))