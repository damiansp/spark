val input = sc.parallelize(List(1, 2, 3, 4, 4));

// map
var result = input.map(x => x * x);
println(result.collect().mkString(","));

// flatMap
val lines = sc.parallelize("hello world", "hey there", "howdy");
val words = lines.flatMap(line => line.split(" "));
println(words.first()); // "hello"

// filter
var odds = input.filter(x => x % 2 != 0);

// distinct
val inputSet = input.distinct();

// sample
val inputSample = input.sample(false, 0.2, 1010); // (withReplacement, fraction, [seed])

val moreInput = sc.parallelize(List(3, 4, 4, 5));

// union
val allInput = input.union(moreInput); // { 1, 2, 3, 4, 4, 3, 4, 4, 5 }
val inputIntersection = input.intersection(moreInput); // { 2, 3, 4, 4 }
val inputDifference1 = input.subtract(moreInput); // { 1, 2 }
val inputCartesian = input.cartesian(moreInput); // { (1, 3), (1, 4), (1, 4), (1, 5), (2, 3) ... }

// reduce
val inputSum = input.reduce((x, y) = x + y); // 14

// fold
val inputProduct = input.fold(1)((x, y) => x * y); // 96; fold(identity)(func)

// aggregate
val res = input.aggregate((0, 0))((acc, value) => (acc._1 + value, acc._2 + 1),
                                  (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2));
val mean = res._1 / res._2;
// aggregate(identityVal)(seqOp, combOp); like reduce bu allows difft return type

// collect
val finalInput = input.collect(); // { 1, 2, 3, 4, 4 }

// count
val inputCount = input.count(); // 5

// countByValue
val inputValCounts = input.countByValue(); // { (1, 1), (2, 1), (3, 1), (4, 2) }

// take, top, takeSample
println(input.take(2)); // { 1, 2 } (any 2)
println(input.top(2)); // { 4, 4 }
println(input.takeSample(true, 3)); // { 4, 3, 3 } (withReplacement, number, [seed])

// foreach
input.foreach(println);



  

