/home/alex/MySoftware/spark-3.0.1-bin-hadoop2.7/bin/spark-submit \
  --class "train.spark.CreateDataFrame" \
  --master local[4] \
  target/scala-2.11/train-spark_2.11-0.0.1.jar
