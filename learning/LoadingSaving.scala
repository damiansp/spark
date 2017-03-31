// Text Files
val input = sc.textFile("path/to/text/file")

// wholeTextFiles() uses file name as a key
val input2 = sc.wholeTextFiles("path/to/text/file")
val result = input2.mapValues{ y =>
  val nums = y.split(" ").map(x => x.toDouble)
  nums.sum / nums.size.toDouble // returns the mean for each file
}

result.saveAsTextFile(outputFile)



// JSON
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

case class Person(name: String, lovesPandas: Boolean)
val result2 = input.flatMap(record => {
  try {
    Some(mapper.readValue(record, classOf[Person]))
  } catch {
    case e: Exception => None
  }
})



// CSV/TSV
// Loading
import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader

val input = sc.textFile(inputFile)
val result = input.map{ line =>
  val reader = new CSVReader(new StringReader(line))
  reader.readNext()
}

// if embedded newlines...
case class Person(name: String, favoriteAnimal: String)
val input = sc.wholeTextFiles(inputFile)
val result = input.flatMap{ case (_, txt) =>
  val reader = new CSVReader(new StringReader(txt))
  reader.readAll().map(x => Person(x(0), x(1)))
}

// saving
pandaLovers.map(person => List(person.name, person.favoriteAnimal).toArray).mapPartitions{ people =>
  val stringWriter = new StringWriter()
  val csvWriter = new CSVWriter(stringWriter)
  csvWriter.writeAll(people.toList)
  Iterator(stringWriter.toString)
}.saveAsTextFile(outFile)



// SequenceFiles
val data = sc.sequenceFile(inFile, classOf[Text], classOf[IntWritable]).map{
  case (x, y) => (x.toString, y.get())
}

// saving
val data = sc.parallelize(List(("Panda", 3), ("Kay", 6), ("Snail", 2)))
data.saveAsSequenceFile(outputFile)

