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

object task4 {
  
  /**
   * Function that sum the distances between the k means points and the other points in the 2D case
   * */
  def sumDistances2D(i : Int) : Double  = {
    println(i)
    val pred = kMeansClusteringXY(i, true) //.groupBy("prediction").agg(mean("X").alias("avgx"), mean("Y").alias("avgy")).show
    val avg : DataFrame= pred.groupBy("prediction").agg(mean("X").alias("avgx"), mean("Y").alias("avgy"))
    val complete : DataFrame= pred.join(avg, pred("prediction") === avg("prediction"), "left_outer")
    //val toreturn = complete
    //.agg(sum((col("X") - col("avgx"))*(col("X") - col("avgx")) + (col("Y") - col("avgy"))*(col("Y") - col("avgy"))).cast("double")).first.getDouble(0)
    //println(toreturn)
    val toreturn = complete
    .agg(sum(sqrt(pow((col("X") - col("avgx")),2.0) + pow((col("Y") - col("avgy")), 2.0)))).first.getDouble(0)
    //println(toreturn)
    return toreturn
  }
  
  /**
   * Function that sum the distances between the k means points and the other points in the 3D case
   * */
  def sumDistances3D(i : Int) : Double  = {
    println(i)
    val pred = kMeansClusteringXYDOW(i, true) //.groupBy("prediction").agg(mean("X").alias("avgx"), mean("Y").alias("avgy")).show
    val avg : DataFrame= pred.groupBy("prediction").agg(mean("X").alias("avgx"), mean("Y").alias("avgy"), mean("dow").alias("avgdow"))
    val complete : DataFrame= pred.join(avg, pred("prediction") === avg("prediction"), "left_outer")
    return complete
   .agg(sum((col("X") - col("avgx"))*(col("X") - col("avgx")) + 
       (col("Y") - col("avgy"))*(col("Y") - col("avgy")) +
       (col("dow") - col("avgdow"))*(col("dow") - col("avgdow"))).cast("double")).first.getDouble(0)
   
  }
  def elbow(dimension :Int) = {
    val range  = 2 to 1000 by 10 toList;
    if(dimension == 2){
      val res = range.map(x => sumDistances2D(x)).zipWithIndex
      //val res1 = spark.sparkContext.parallelize(res).toDF("dist" , "ind")
      val res1 = spark.createDataFrame(res).toDF("dist", "ind")
      
    //res1.coalesce(1).write.csv("/results/elbow2.csv")
      res1.coalesce(1)
         .write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save("results/elbow2.csv") 
    
    
    }else if(dimension == 3){
      val res = range.map(x => sumDistances3D(x)).zipWithIndex
      val res1 = spark.createDataFrame(res).toDF("dist", "ind")
      res1.coalesce(1)
         .write
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .mode("overwrite")
         .save("results/elbow3.csv")
    }
   
    
  }
  
  def run(){
    
    
    
  }
  
}