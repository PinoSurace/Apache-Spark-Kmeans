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


object task1 extends DFtask{
  
  /**
 * Function implements the k means clustering for task 1.
 * */               
                   
  def kMeansClustering(data: DataFrame, n : Int, elbow : Boolean = false) : DataFrame = {
     
    val vectorAssembler = new VectorAssembler().setInputCols(Array("X", "Y")).setOutputCol("features")                              
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
    val coordinates : DataFrame = data.select("X", "Y")                                  
    val pipeLine = transformationPipeline.fit(coordinates)
    val transformedTraining = pipeLine.transform(coordinates)
    val kmeans = new KMeans().setK(n).setSeed(1L)
    val kmModel = kmeans.fit(transformedTraining)
    return kmModel.summary.predictions  
  }
  
  def printDF(data : DataFrame, filename: String) = {
    val header = Array("X", "Y")
    val rows = data.rdd.map( row =>   Array(row(0).toString(), row(1).toString())).collect()
    val allRows = header +: rows
    val csv = allRows.map(_.mkString(",")).mkString("\n")
    new PrintWriter(filename) {write(csv); close()}
    
    /*
    
    data
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .format("csv")//.format("com.databricks.spark.csv")
    .option("header", "true")    
    .save("results/basic.csv")*/
    
  }
  
  def readCSVtoDF(spark : SparkSession) : DataFrame = {
    val data : DataFrame  = spark.read 
                   .format("csv")
                   .option("delimiter", ";")
                   .option("header", "true")
                   .option("inferSchema", "true")
                   .load("data/tieliikenneonnettomuudet_2015_onnettomuus.csv")
                   .select("X", "Y")
                   .na.drop
    
    return data    
  }
  
  def getExtremeValuesFromDF(data :DataFrame) : (Int, Int, Int, Int,Double,Double)  = {
    val minMaxOfColumns = data.agg(min("X").as("min_X"), max("X").as("max_X"), 
                                   min("Y").as("min_Y"), max("Y").as("max_Y"))
  
    val min_X : Int= minMaxOfColumns.select("min_X").first().getInt(0) 
    val max_X : Int= minMaxOfColumns.select("max_X").first().getInt(0) 
    val min_Y : Int= minMaxOfColumns.select("min_Y").first().getInt(0)
    val max_Y : Int= minMaxOfColumns.select("max_Y").first().getInt(0)
    
    return (min_X, max_X, min_Y, max_Y, 0, 0)
  }
  
  def scaleData(data :DataFrame, min_X : Int, max_X : Int, min_Y :Int, max_Y : Int, min_DOW : Double = 0, max_DOW : Double = 0) : DataFrame = {
       
    val range_X : Int= max_X - min_X
    val range_Y : Int= max_Y - min_Y    
    val df_scaled = data.withColumn("X_scaled",((col("X") - min_X)/range_X))
                      .withColumn("Y_scaled",((col("Y") - min_Y)/range_Y))
    
    return df_scaled
  }
  
  
  def rescaleData(data :DataFrame, min_X : Int, max_X : Int, min_Y :Int, max_Y : Int, min_DOW : Double = 0, max_DOW : Double = 0) : DataFrame = {
    val range_X : Int= max_X - min_X
    val range_Y : Int= max_Y - min_Y
    
    return data.groupBy("prediction")
     .agg((mean("X")*range_X + min_X).as("x"), (mean("Y")*range_Y + min_Y).as("y")  ).drop("prediction")
    
    
  }
  
  
  
  def run(spark : SparkSession) = {
    
    val df = readCSVtoDF(spark)
    val (min_X, max_X, min_Y, max_Y, _, _) = getExtremeValuesFromDF(df)
    
    val df_scaled = scaleData(df, min_X , max_X, min_Y , max_Y )
    
    val clusters = kMeansClustering(df_scaled, 5)
    
    val df_rescaled = rescaleData(clusters, min_X , max_X, min_Y , max_Y )
    printDF(df_rescaled, "results/basic.csv")
    
  }
}