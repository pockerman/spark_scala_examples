package train.spark

/*

Explore the DataFrame Column API

*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object ExploreDataFrameAPI {

  
  def main(args: Array[String]) {
  
    val csvFile = "/home/alex/qi3/learn_scala/scripts/spark/data/train.csv" 
    val appName: String = "Spark DataFrame API Demo"
    
    val spark = SparkSession
    .builder()
    .appName(appName)
    .getOrCreate()
    
    // specify the schema 
    val customSchema = StructType(Array(
			StructField("mu-1", DoubleType, false),
  			StructField("mu-2", DoubleType, false),
	  		StructField("label", IntegerType, false)))
    
    // read the data frame
    val df = spark.read.schema(customSchema).csv(csvFile)
    
    // print the schema
    df.printSchema()
    
    // get the columns
    df.columns
    
    // access a particular column by using col
    // it returns a Column type
    val colId = df.col("Id")
    
    // we can use expressions on columns
    df.select(expr("Hits * 2")).show(2)
    
    // compute a value
    df.select(col("Hits") * 2).show(2)
    
    
    // add a new column in the data frame
    df.withColumn("Big Hitters", (expr("Hits > 10000"))).show()	
    
    // concatenate columns and create a new column
    df.withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id"))))
      .select(col("AuthorsId"))
      .show(4)
        
  }
}
