package train.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object CreateDataFrame {

  
  def main(args: Array[String]) {
  
    val csvFile = "/home/alex/qi3/learn_scala/scripts/spark/data/train.csv" 
    val appName: String = "Spark DataFrame Demo"
    
    /*val spark = SparkSession
    .builder()
    .appName(appName)
    .getOrCreate()
    
    
    val df = spark.read.csv(csvFile)
    
    // print the schema
    df.printSchema()
    */
    
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    val customSchema = StructType(Array(
			StructField("mu-1", DoubleType, false),
  			StructField("mu-2", DoubleType, false),
	  		StructField("label", IntegerType, false)))
    
    // specify a schema
    val df_schema = sqlContext.read.format("csv")
    			  .option("delimiter",",")
    			  .schema(customSchema)
    			  .load(csvFile)
    			  
    df_schema.printSchema()	
    
    df_schema.groupBy("label").count().show()	
    df_schema.show(5)	
    	  
   
  }
}
