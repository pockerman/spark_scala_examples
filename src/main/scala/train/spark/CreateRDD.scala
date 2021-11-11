package train.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object CreateRDD {
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Spark RDD Demo")
    val sc = new SparkContext(conf)
    
    val data = Array(1,2,3,4,5,6,7,8,9,10)
    val rdd = sc.parallelize(data)
    rdd.foreach(println)
    
    println("Number of Partitions: "+rdd.getNumPartitions)
    println("Action: First element: "+rdd.first()) 
  }
}
