package train.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

object CreateRDD_2 {
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Hello Scala Spark")
    val sc = new SparkContext(conf)
    
    println("Spark version:            " + sc.version)
    println("Spark master:             " + sc.master)
    println("Spark running 'locally'?: " + sc.isLocal)
    
    val sqlContext = new SQLContext(sc)
    
    // create the first RDD
    // Should be some file on your system
    val csvFile = "/home/alex/qi3/learn_scala/scripts/spark/data/train.csv" 
    
    val data = sqlContext.read.csv(csvFile).cache()
    println(s"Is original data cached? $data.is_cached") 
    
    // how many lines
    val nLines = data.count()
    
    println(s"Number of lines $nLines")
    
    // try to read the lines but not the header
    //val dataNoHeader = data.filter(line => line.contains("#")).cache()
    
    //data.unpersist()
    
    //println(s"Is original data cached? $data.is_cached") 
    //println(s"Number of new lines $dataNoHeader.count()") 
  }
}
