package train.spark

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.DoubleType


object LinearRegressionApp {

  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Linear regression Spark")
    val sc = new SparkContext(conf)
    
    val session = SparkSession.builder().appName("Linear regression Spark").master("local[4]").getOrCreate()
    
    // Should be some file on your system
    val csvFile = "/home/alex/qi3/spark_scala/data/spark_regression.csv" 
    val inputTrainigSet = session.read.format("csv").load(csvFile)
        
    println("Number of Partitions: "+inputTrainigSet.rdd.getNumPartitions)
    println("Action: First element: "+inputTrainigSet.rdd.first()) 
   
   val analysisData  = inputTrainigSet.withColumn("x1", inputTrainigSet("_c0").cast(DoubleType))
                                      .withColumn("x2", inputTrainigSet("_c1").cast(DoubleType))
                                      .withColumn("y",  inputTrainigSet("_c2").cast(DoubleType)) 
                                      .drop("_c0")
                                      .drop("_c1")
                                      .drop("_c2")
   
    
   //creating features column
   val assembler = new VectorAssembler()
  			.setInputCols(Array("x1","x2"))
  			.setOutputCol("features")
    
    // create the model
    val lr = new LinearRegression()
  		.setMaxIter(10)
  		.setRegParam(0.3)
  		.setElasticNetParam(0.8)
  		.setFeaturesCol("features")
  		.setLabelCol("y")
  		
   val trainigSet = assembler.transform(analysisData)
  		
   // Fit the model
   val lrModel = lr.fit(trainigSet)

  // Print the coefficients and intercept for linear regression
  println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

  // Summarize the model over the training set and print out some metrics
  val trainingSummary = lrModel.summary
  
  println(s"numIterations: ${trainingSummary.totalIterations}")
  
  // there is sth wrong with my scala/spark version and this
  // throws an excpetion
  //println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
  
  trainingSummary.residuals.show()
  println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
  println(s"r2: ${trainingSummary.r2}")
}
}
