package train.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object LoadCSV {
  def main(args: Array[String]) {
    
    val spark = SparkSession.builder.appName("Create RDD").getOrCreate()
    
    //println("Spark version:            " + spark.version)
    //println("Spark master:             " + spark.master)
    //println("Spark running 'locally'?: " + spark.isLocal)
    
    // create the first RDD
    // Should be some file on your system
    val csvFile = "/home/alex/qi3/learn_scala/scripts/spark/data/train.csv" 
    
    val data = spark.read.format("csv").option("header", "true").load(csvFile).cache()
    
    data.show()
    println(s"Is original data cached? $data.is_cached") 
    
    // how many lines
    val nLines = data.count()
    
    println(s"Number of lines $nLines")
    
    // try to read the lines but not the header
    //val dataNoHeader = data.filter(line => line.contains("#")).cache()
    
    data.unpersist()
    
    println(s"Is original data cached? $data.is_cached") 
    //println(s"Number of new lines $dataNoHeader.count()") 
    spark.close()
  }
}
