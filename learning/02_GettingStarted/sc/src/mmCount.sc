package main.scala.chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
 * Usage: mmCount <path>
 */
object MMCount {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("MMCount").getOrCreate()
    if (args.length < 1) {
      print("Usage: mmCount <path>")
      sys.exit(1)
    }
    val mmFile = args(0)
    val mmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mmFile)
    val countMMDF = mmDF.select("State", "Color", "Count")
      .groupBy("State", "Color")
      .sum("Count")
      .orderBy(desc("sum(Count)"))
    countMMDF.show(30)
    println(s"Total Rows: ${countMMDF.count()}")
    println()
    val caCountMMDF = mmDF.select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .sum("Count")
      .orderBy(desc("sum(Count)"))
    caCountMMDF.show(10)
    spark.stop()
  }
}