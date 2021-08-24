package train.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object HelloSpark {
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Hello Scala Spark")
    val sc = new SparkContext(conf)
    println("Spark version:            " + sc.version)
    println("Spark master:             " + sc.master)
    println("Spark running 'locally'?: " + sc.isLocal)
  }
}



