import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{
  OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions.date_format


staticDF.printSchema()

val preppedDF = staticDF
  .na.fill(0)
  .withColumn("day_of_week", date_format($"InvoiceDate", "EEEE"))
  .coalesce(5)
val trainDF = preppedDF.where("InvoiceDate < '2011-07-01'")
val testDF  = preppedDF.where("InvoiceDate >= '2011-07-01'")

println(trainDF.count())
println(testDF.count())

val indexer = new StrinIndexer()
  .setInputCol("day_of_week")
  .setOutputCol("day_of_week_index")
val encoder = new OneHotEncoder()
  .setInputCol("day_of_week_index")
  .setOutputCol("day_of_week_encoded")
val vectorAssembler = new VectorAssembler()
  .setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded"))
  .setOutputCol("features")
val transformationPipeline = new Pipeline()
  .setStages(Array(indexer, encoder, vectorAssembler))
val fittedPipeline = transformationPipeline.fit(trainDF)
val transformedTraining = fittedPipeline.transform(trainDF)
transformedTraining.cache()

val kmeans = new KMeans().setK(20).setSeed(1L)
val kmModel = kmeans.fit(transformedTraining)
kmModel.computeCost(transformedTraining)

val transformedTest = fittedPipeline.transfrom(testDF)
kmModel.computeCost(transformedTest)