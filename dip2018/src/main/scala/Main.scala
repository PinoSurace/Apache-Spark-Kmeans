import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{ArrayType, StringType, StructField}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{sum, min, max, asc, desc, udf, mean}
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.lang.Thread

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField,StructType, StringType, LongType, IntegerType, DoubleType, Metadata}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.MinMaxScaler
import scala.annotation.tailrec
import scala.collection._
import org.apache.spark.rdd.RDD
import java.util.Date
import java.text.SimpleDateFormat

import scala.util.Random
import scala.collection.immutable.HashSet
import scala.util.control.Breaks._
import java.io.PrintWriter
import org.apache.spark.sql.SaveMode

//import org.apache.spark.sql.SQLContext.implicits._
//import org.apache.spark.ml.feature.Imputer








object main extends App {
  
  if (args.length == 0) {
        println("error, parameters missing")
   }
  else if (args(0) == "-task"){
    // Suppress the log messages:
    Logger.getLogger("org").setLevel(Level.OFF)

	  val spark = SparkSession.builder()
                          .appName("Assignment")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "5")
  
    import spark.implicits._
  
    args(1) match {
      case "1" => task1.run(spark)
      case "2" => task2.run(spark)
      case "3" => println("It is in the document")
      case "4" => task4.run(spark)
      case "5" => task5.run(spark)
      case "6" => task6.run(spark) 
      
    }
    
  } 
}