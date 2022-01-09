DATA=../../../data
sbt clean package
spark-submit --class main.scala.chapter2.MMCount \
             target/scala-2.12/main-scala-chapter2_2.12-1.0.jar \
             $DATA/mm.csv
