package train.spark

import scala.util.{Try, Success, Failure}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object CreateRDDFile {
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Hello Scala Spark")
    val sc = new SparkContext(conf)
    
    // Should be some file on your system
    val csvFile = "/home/alex/qi3/learn_scala/scripts/spark/data/train.csv" 
    val csvRDD = sc.textFile(csvFile)
        
    println("Number of Partitions: "+csvRDD.getNumPartitions)
    println("Action: First element: "+csvRDD.first()) 
    
    val doubleRDD = csvRDD.map(line => {line.split(",")})
                           .map( arrString => {Try(Array(arrString(0).toDouble, arrString(1).toDouble, arrString(2).toDouble))})
                           .map(_ match {case Success(res) => res
                                         case Failure(res) => Array(-100, -100, -100)})
                                          
  }
}
