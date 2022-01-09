package main.scala.chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
 * Usage: MMCount [mm_dataset_file]
 */
object MMCount {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("MMCount").getOrCreate()
    if (args.length < 1) {
      println("Usage: MMCount [mm_dataset_file]")
      sys.exit(1)
    }
    val mmFile = args(0)
    val mmDF = spark
      .read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mmFile)
    val  countMMDF = mmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .sum("Count")
      .orderBy(desc("sum(Count)"))
    countMMDF.show(60)
    println(s"Total Rows: ${countMMDF.count()}")
    println()
    val caCounts = mmDF
      .select("*")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .sum("Count")
      .orderBy(desc("sum(Count)"))
    caCounts.show(10)
    spark.stop()
  }
}
