// Creating Pair RDDs
val pairs = lines.map(x => (x.split(" ")(0), x)); // use first word per line as key


// Transformations on Pair RDDs
// example data: rdd = { (1, 2), (3, 4), (3, 6) }

// reduceByKey(func); foldByKey(identity)(func)
rdd.reduceByKey((x, y) => x + y); // { (1, 2), (3, 10) }
rdd.foldByKey(1)(x, y) => x * y); // { (1, 2), (3, 24) }
rdd.groupByKey();                 // { (1, [2]), (3, [4, 6]) }

// combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner)

// mapValues(func)
rdd.mapValues(x => x * x);        // { (1, 4), (3, 16), (3, 36) }

// flatMapValues(func)
rdd.flatMapValues(x => x to 5);   // { (1, 2), (1, 3), (1, 4), (1, 5), (3, 4), (3, 5) }
rdd.keys();                       // { 1, 3, 3 }
rdd.values();                     // { 2, 4, 6 }
rdd.sortByKey();                  // { (1, 2), (3, 4), (3, 6) }



// Two Pair RDDs
// other = { (3, 9) }
rdd.subtractByKey(other); // { (1, 2) }
rdd.join(other);          // { (3, (4, 9)), (3, (6, 9)) }
rdd.rightOuterJoin(other); // { (3, (Some(4), 9)), (3, (Some(6), 9)) }
rdd.leftOuterJoin(other);  // { (1, (2, None), (3, (4, Some(9))), (3, (6, Some(9))) }
rdd.cogroup(other);        // { (1, ([2], [])), (3, ([4, 6], [9])) }

// Word count example
val input = sc.textFile("s3://path/to/file.txt");
val words = input.flatMap(x => x.split(" "));
val result = words.map(x => (x, 1)).reduceByKey((x, y) => x + y);

// or
val result2 = input.flatMap(x => x.split(" ")).countByValue();

// Per-key average using combineByKey()
val result = input.combineByKey(
  // createCombiner
  (v) => (v, 1),
  // mergeValue
  (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
  // mergeCombiners
  (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
  .map{ case (key, value) => (key, value._1 / value._2.toFloat) };

result.collectAsMap().foreach(println(_));


 
// Joins
val storeAddress = { (Store("Ritual"), "1026 Valencia"),
                     (Store("Philz"), "748 Van Ness"),
                     (Store("Philz"), "3101 24th"),
                     (Store("Starbucks"), "Seattle") };
val storeRating = { (Store("Ritual"), 4.9), (Store("Philz"), 4.8) };
val storeAddressRating = storeAddress.join(storeFating);
// { (Store("Ritual"), (1026 Valencia, 4.9)), ... }



// Sorting
val input: RDD[(Int, Venue)] = ...;
implicit val sortIntegersByString = new Ordering[Int] {
  override def compare(a: Int, b: Int) = a.toString.compare(b.toString);
};
rdd.sortByKey();



// Actions on Pair RDDs
// ex = { (1, 2), (3, 4), (3, 6) }
rdd.countByKey(); // { (1, 1), (3, 2) }
rdd.collectAsMap(); // Map{ (1, 2), (3, 4), (3, 6) }
rdd.lookup(3); // [4, 6]



// Data Partitioning
// Init. code: load user infor from a Hadoop SequenceFile on HDFS. This distributes elems of
// userData by the HDFS block where they are found, and doesn't provide Spark w any way of knowing
// in which a partition a particular UserID is located
val sc = new SparkContext(...);
val userData = sc.sequenceFile[UserID, UserInfo]("hdfs://...").persist();

// Func called periodically to process a logfile of events in the past 5 mins-- assumed to be a
// SequenceFile containing (UserID, LinkInfo) pairs
def processNewLogs(logFileName: String) {
  val events = sc.sequenceFile[UserID, LinkInfo](logFileName);
  val joined = userData.join(events); // RDD: (UserID, (UserInfor, LinkInfo))
  val offTopicVisits = joined.filter {
    // Expand tuple into compoents
    case (userId, (userInfo, linkInfo)) => !userInfo.topics.contains(linkInfo.topic);
  }.count();
  println("Number of visits to no-subscribed topics: " + offTopicVisits);
}

// inefficient: join used every time func is called, wo any knowledge of partitioning. Improve:
import org.apache.spark.HashPartitioner;

val userData = sc.sequenceFile[UserID, LinkInfo]("hdfs://...")
  .partitionBy(new HashPartitioner(100)) // for 100 partitions
  .persist();



// Determining and RDD's Partitioner
val pairs = sc.parallelize(List((1, 1), (2, 2), (3, 3)));
pairs.partitioner; // None
val partitioned = pairs.partitionBy(new HashPartitioner(2));
partitioned.partitioner; // Some(spark.HashPartitioner...)
