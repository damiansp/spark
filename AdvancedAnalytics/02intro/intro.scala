// Run in REPL
import java.lang.Double.isNaN
import org.apache.spark.rdd.RDD
import org.apache.spark.util.StatCounter

val rawblocks = sc.textFile("linkage")
val head = rawblocks.take(10)

def isHeader(line: String) = line.contains("id_1")

val noHeader = rawblocks.filter(x => !isHeader(x))

noHeader.first

val line = head(5)
val pieces = line.split(',')
val rawscores = pieces.slice(2, 11)

def toDouble(s: String) = {
  if ("?".equals(s)) Double.NaN else s.toDouble
}

val scores = rawscores.map(toDouble)

def parse(line: String) = {
  val pieces = line.split(',')
  val id1 = pieces(0).toInt
  val id2 = pieces(1).toInt
  val scores = pieces.slice(2, 11).map(toDouble)
  val matched = pieces(11).toBoolean
  (id1, id2, scores, matched)
}

val tup = parse(line)

case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)

// Better:
def parse(line: String) = {
  val pieces = line.split(',')
  val id1 = pieces(0).toInt
  val id2 = pieces(1).toInt
  val scores = pieces.slice(2, 11).map(toDouble)
  val matched = pieces(11).toBoolean
  MatchData(id1, id2, scores, matched)
}

val md = parse(line)

// Access values by name
md.matched
md.id1

// Apply to all but header
val mds = head.filter(x => !isHeader(x)).map(x => parse(x))
val parsed = noHeader.map(line => parse(line))
parsed.cache()


/* Aggregations */
val grouped = mds.groupBy(md => md.matched)
grouped.mapValues(x => x.size).foreach(println)


/* Creating Histograms */
val matchCounts = parsed.map(md => md.matched).countByValue()
val matchCountsSeq = matchCounts.toSeq

matchCountsSeq.sortBy(_._1).foreach(println)
matchCountsSeq.sortBy(_._2).reverse.foreach(println)


/* Summary Stats for Continuous Variables */
parsed.map(md => md.scores(0)).stats() // generates count, mean, sd, min, max, but NaNs if NaNs...

parsed.map(md => md.scores(0)).filter(!isNaN(_)).stats()

// Streamline for all fields
val stats = (0 until 9).map(i => {
  parsed.map(md => md.scores(i)).filter(!isNaN(_)).stats()
})

stats(1)


/* Creating Reusable Code for Computing Summary Stats */
class NAStatCounter extends Serializable {
  val stats: StatCounter = new StatCounter()
  var missing: Long = 0

  def add(x: Double): NAStatCounter = {
    if (java.lang.Double.isNaN(x)) {
      missing += 1
    } else {
      stats.merge(x)
    }

    this
  }

  def merge(other: NAStatCounter): NAStatCounter = {
    stats.merge(other.stats)
    missing += other.missing
    this
  }

  override def toString = {
    "Stats: " + stats.toString + " NaN: " + missing
  }
}

object NAStatCounter extends Serializable {
  def apply(x: Double) = new NAStatCounter().add(x)
}

val nas1 = NAStatCounter(10.0)
nas1.add(2.1)

val nas2 = NAStatCounter(Double.NaN)
nas1.merge(nas2)


val arr = Array(1.0, Double.NaN, 17.29)
val nas = arr.map(d => NAStatCounter(d))
val nasRDD = parsed.map(md => {
  md.scores.map(d => NAStatCounter(d))
})

val nas1 = Array(1.0, Double.NaN).map(d => NAStatCounter(d))
val nas2 = Array(Double.NaN, 2.0).map(d => NAStatCounter(d))
val merged = nas1.zip(nas2).map(p => p._1.merge(p._2)) // or
val merged = nas1.zip(nas2).map { case (a, b) => a.merge(b) }

val nas = List(nas1, nas2)
val merged = na.reduce((n1, n2) => {
  n1.zip(n2).map { case (a, b) => a.merge(b) }
})

val reduced = nasRDD.reduce((n1, n2) => {
  n1.zip(n2).map { case (a, b) => a.merge(b) }
})

// Encapsulate
def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
  val naStats = rdd.mapPartitions((iter: Iterator[Array[Double]]) => {
    val nas: Array[NAStatCounter] = iter.next().map(d => NAStatCounter(d))

    iter.foreach(arr => {
      nas.zip(arr).foreach { case (n, d) => n.add(d) }
    })

    Iterator(nas)
  })

  naStats.reduce((n1, n2) => {
    n1.zip(n2).map { case (a, b) => a.merge(b) }
  })
}
