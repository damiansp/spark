import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.appName("PeopleAges").getOrCreate()
val data = Seq(("Bobby", 20), ("Bobby", 25), ("Denny", 31), ("Jules", 30), ("Elmer", 64))
val dataDF = spark.createDataFrame(data).toDF("name", "age")
val avgDF = dataDF.groupBy("name").agg(avg("age"))
avgDF.show()