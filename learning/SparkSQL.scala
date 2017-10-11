/* Using Spark SQL in Applications
 * Initializing Spark SQL
 */
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext // if can't have Hive dependencies

// Construct a SQL context
val conf = new SparkConf().setAppName('myApp')
val sc = new SparkContext(conf)

// Create Spark SQL Hive Context
val hiveCtx = new HiveContext(sc)
// Import implicit conversions
import hiveCtx.implicits._

// or
val sqlContext = SQLContextSingleton.getInstance(sc)


// Newer approach:
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.getOrCreate()
import spark.implicits._



/* Basic Query Example */
val input = hiceCtx.jsonFile(inputFile)
input.registerTempTable("tweets") // register the input schema RDD
val topTweets = hiveCtx.sql(
  """
     SELECT text, retweetCount
     FROM tweets
     ORDER BY retweetCount
     LIMIT 10
  """)


/* Dataframes */
// Common operations
df.show()
df.select("name", df("age") + 1)
df.filter(df("age") > 19)
df.groupBy(df("name")).min() // max, mean, agg, etc

// convert DF to RDD
val topTweetText = topTweets.rdd().map(row => row.getString(0))


/* From RDDs */
case class HappyPerson(handle: String, favoriteDrink: String)

val happyPeopleRDD = sc.parallelize(List(HappyPerson("Michael Stipe", "boba tea")))
// implicit conversion equiv to sqlCtx.createDataFrame(happyPeopleRDD)
happyPeopleRDD.registerTempTable("happyPeople")


/* UDFs (User-Defined Funtions) */