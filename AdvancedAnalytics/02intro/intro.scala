// Run in REPL
val rawblocks = sc.tetxtFile("linkage")
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