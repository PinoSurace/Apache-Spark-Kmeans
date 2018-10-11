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
  def sumDistances2D(data: DataFrame, i : Int) : Double  = {
    println(i)
    val pred = task1.kMeansClustering(data, i) //.groupBy("prediction").agg(mean("X").alias("avgx"), mean("Y").alias("avgy")).show
    val avg : DataFrame= pred
                         .groupBy("prediction")
                         .agg(mean("X_scaled").alias("avgx"),
                              mean("Y_scaled").alias("avgy"))
    val complete : DataFrame= pred
                              .join(avg, pred("prediction") === avg("prediction"), "left_outer")
    
    val toreturn = complete
                    .agg(sum(sqrt(pow((col("X_scaled") - col("avgx")),2.0) + 
                                  pow((col("Y_scaled") - col("avgy")), 2.0))))
                                  .first.getDouble(0)
    
    return toreturn
  }
  
  /**
   * Function that sum the distances between the k means points and the other points in the 3D case
   * */
  def sumDistances3D(data: DataFrame, i : Int) : Double  = {
    println(i)
    val pred = task2.kMeansClustering(data,i) 
    val avg : DataFrame= pred
                       .groupBy("prediction")
                       .agg(mean("X_scaled").alias("avgx"),
                            mean("Y_scaled").alias("avgy"),
                            mean("dow_x_scaled").alias("avgdow_x"),
                            mean("dow_y_scaled").alias("avgdow_y"))
    val complete : DataFrame= pred
                              .join(avg, pred("prediction") === avg("prediction"), "left_outer")
    return complete
           .agg(sum(sqrt(pow((col("X_scaled") - col("avgx")),2.0) + 
                         pow((col("Y_scaled") - col("avgy")), 2.0)+
                         pow((col("dow_x_scaled") - col("avgdow_x")), 2.0)+
                         pow((col("dow_y_scaled") - col("avgdow_y")), 2.0))))
                         .first.getDouble(0)
  }
  
  
  def toCSV_elbow(costs : List[(Int, Double)], filename: String = "result/task6.csv") {
    val header = Array("NumOfCluster", "Cost")
    val rows = costs.map(x => Array(x._1.toString(), x._2.toString()))
    val allRows = header +: rows
    val csv = allRows.map(_.mkString(",")).mkString("\n")
    new PrintWriter(filename) {write(csv); close()}
  }
  
  def elbow(data: DataFrame, dimension :Int) = {
    val range  = 2 to 302 by 10 toList;
    if(dimension == 2){
      val res = range.map(x => (x,sumDistances2D(data,x)))
      toCSV_elbow(res,  "results/elbow2D.csv")
          
    }else if(dimension == 3){
      
      val res = range.map(x => (x,sumDistances3D(data,x)))
      toCSV_elbow(res,  "results/elbow3D.csv")
      
    }
   
    
  }
  
  def runKMeansDim2(spark : SparkSession){
    println("-------------------task 4 dim2 start----------------")
    val dataXY = task1.readCSVtoDF(spark)
    val (min_X, max_X, min_Y, max_Y) = task1.getExtremeValuesFromDF(dataXY)
    val df_scaled = task1.scaleData(dataXY, min_X , max_X, min_Y , max_Y )
    elbow(df_scaled, 2)
    
    println("-------------------task 4 dim2 end----------------")
    println("-------------------task 4 dim3 start----------------")
    println("-------------------task 4 dim3 end----------------")
    
  }
  
  def runKMeansDim3(spark : SparkSession){
    println("-------------------task 4 dim3 start----------------")
    val dataXYDOW = task2.readCSVtoDF(spark)
    val (min_X, max_X, min_Y, max_Y, min_dow_x, max_dow_x,min_dow_y, max_dow_y) = task2.getExtremeValuesFromDF(dataXYDOW)
    
    val df_scaled = task2.scaleData(dataXYDOW, min_X , max_X, min_Y , max_Y, min_dow_x, max_dow_x,min_dow_y, max_dow_y )
    
    elbow(df_scaled, 3)
    
    println("-------------------task 4 dim3 end----------------")    
    
  }
  
  def run(spark : SparkSession){
    runKMeansDim2(spark)
    runKMeansDim3(spark)
    
  }
  
}