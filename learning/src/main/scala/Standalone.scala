import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

// spark-submit --master 'local' --class main.scala.x

object Standalone {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("App Name")
    val sc = new SparkContext(conf)
  }
}


