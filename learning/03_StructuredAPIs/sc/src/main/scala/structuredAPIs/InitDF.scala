package main.scala.structuredAPIs

import org.apache.spark.sql.functions._ //{col, concat, expr}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._


object InitDF {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("InitDF").getOrCreate()
    import spark.implicits._

    if (args.length < 1) {
      println("Usage: InitDF [path to blogs.json]")
      System.exit(1)
    }
    val jsonFile = args(0)
    val schema = StructType(
      Array(
        StructField("ID", IntegerType, false),
        StructField("First", StringType, false),
        StructField("Last", StringType, false),
        StructField("URL", StringType, false),
        StructField("Published", StringType, false),
        StructField("Hits", IntegerType, false),
        StructField("Campaigns", ArrayType(StringType), false)))
    val df = spark.read.schema(schema).json(jsonFile)
    df.show(false)
    println(df.printSchema)
    println(df.schema)
    df.select(expr("Hits") * 2).show(2)
    df.select(col("Hits") * 2).show(2)
    df.select(expr("Hits * 2")).show(2)
    df.withColumn("Big Hitters", (expr("Hits > 10000"))).show()
    df.withColumn("AuthorID", (concat(expr("First"), expr("Last"), expr("ID"))))
      .select(col("AuthorID"))
      .show(4)
    df.sort(col("ID").desc).show()
    //df.sort($"ID".desc).show()

    val dfRow = Row(
      7, "Bob", "Dobolina", "https://bobdob.com", 98987, "11/3/1976", Array("web", "LinkedIn"))
    println(dfRow(2))

    val rows = Seq(("Bob Dobolina", "NY"), ("Maude Dobson", "CA"))
    val df2 = rows.toDF("Person", "Place")
    df2.show()
  }
}