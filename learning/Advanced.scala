// Accumulators
// Accumulator for an empty-line count
val file = sc.textFile("file.txt")
val blankLines = sc.accumulator(0) // Create an Accumulator[Int], initialized to 0
val callSigns = file.flatMap(line => {
  if (line == "") blankLines += 1
  lines.split(" ")
})

callSigns.saveAsTextFile("output.txt")
println("Blank lines: " + blankLines.value)
