DATA=./data
sbt clean package
spark-submit --class main.scala.structuredAPIs.InitDF \
             target/scala-2.12/main-scala-structuredapis_2.12-1.0.jar \
             $DATA/blogs.json

