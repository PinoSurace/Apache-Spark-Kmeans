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

object task2 {
  
  /**
 	* Function implements the k means clustering for 4 dimensions.
 	* */            
  def kMeansClustering(data: DataFrame, n : Int) : DataFrame = {     
     
    val vectorAssembler = new VectorAssembler().setInputCols(Array("X_scaled", "Y_scaled", "dow_x_scaled", "dow_y_scaled")).setOutputCol("features") 
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))   
    val coordinates : DataFrame = data.select("X_scaled", "Y_scaled", "dow_x_scaled", "dow_y_scaled")
    val pipeLine = transformationPipeline.fit(coordinates)
    val transformedTraining = pipeLine.transform(coordinates)
    val kmeans = new KMeans().setK(n).setSeed(1L)
    val kmModel = kmeans.fit(transformedTraining) 
    
    return kmModel.summary.predictions
  }
  
  /**
   * Write input dataframe to csv file with path specified in filename
   * 
   **/  
  def printDFtoCSV(data : DataFrame, filename: String) = {
    val header = Array("x", "y", "dow")
    val rows = data.rdd.map( row =>   Array(row(0).toString(), row(1).toString(), row(2).toString())).collect()
    val allRows = header +: rows
    val csv = allRows.map(_.mkString(";")).mkString("\n")
    new PrintWriter(filename) {write(csv); close()}
    
  }
  
  
  /**
   * Read data from csv file and transform it so that day of week are not string,
   * but numbers transformed in points with two coordinates x is the cosine and y is the sine of the
   * angle formed by the respective weekday.
   * Moreover dirty data are removed.
   * */
  def readCSVtoDF(spark : SparkSession) : DataFrame = {    
                   
    val dfXYVkpv : DataFrame  = spark.read 
                   .format("csv")
                   .option("delimiter", ";")
                   .option("header", "true")
                   .option("inferSchema", "true")
                   .load("data/tieliikenneonnettomuudet_2015_onnettomuus.csv")
                   .select("X", "Y", "Vkpv" )
                   .na.drop

 /**
  * Create a new dataframe where days of the week are mapped 
  * 
  * */                  
                  //when otherwise
  val weekDays = Seq(
  Row("Maanantai" : String, math.cos(1.0 * 2*math.Pi/ 7) : Double, math.sin(1.0 * 2*math.Pi/ 7) : Double),
  Row("Tiistai" : String, math.cos(2.0*2* math.Pi/ 7) : Double, math.sin(2.0 * 2*math.Pi/ 7) : Double),
  Row("Keskiviikko" : String, math.cos(3.0*2* math.Pi/ 7) : Double, math.sin(3.0 * 2*math.Pi/ 7) : Double),
  Row("Torstai" : String, math.cos(4.0*2* math.Pi/ 7) : Double, math.sin(4.0 * 2*math.Pi/ 7) : Double),
  Row("Perjantai" : String, math.cos(5.0*2* math.Pi/ 7) : Double, math.sin(5.0 * 2*math.Pi/ 7) : Double),
  Row("Lauantai" : String,math.cos(6.0*2* math.Pi/ 7) : Double, math.sin(6.0 * 2*math.Pi/ 7) : Double),
  Row("Sunnuntai" : String,math.cos(7.0* 2*math.Pi/ 7) : Double, 0 : Double))

  val weekDaysSchema = List(
    StructField("Vkpv", StringType, true),
    StructField("dow_x", DoubleType, true),
    StructField("dow_y", DoubleType, true)
  )

  val weekDaysDF = spark.createDataFrame(
    spark.sparkContext.parallelize(weekDays),
    StructType(weekDaysSchema)
  )  
  
/**
 * Create a dataframe with coordinates X and Y and two coordinates of day of the week
 * 
 * 
 * */  
    val data : DataFrame= dfXYVkpv.join(weekDaysDF, dfXYVkpv("Vkpv") === weekDaysDF("Vkpv"), "left_outer").drop("Vkpv")
                    
    
    return data    
  }
  
  /**
   *  Return the extreme values of the columns of dataframe
   * */
  def getExtremeValuesFromDF(data :DataFrame) : (Int, Int, Int, Int, Double, Double, Double, Double) = {
    val minMaxOfColumns = data.agg(min("X").as("min_X"), max("X").as("max_X"), 
                                min("Y").as("min_Y"), max("Y").as("max_Y"),
                                min("dow_x").as("min_dow_x"), max("dow_x").as("max_dow_x"),
                                min("dow_y").as("min_dow_y"), max("dow_y").as("max_dow_y"))
  
    val min_X  : Int= minMaxOfColumns.select("min_X").first().getInt(0) 
    val max_X : Int= minMaxOfColumns.select("max_X").first().getInt(0) 
    val min_Y : Int= minMaxOfColumns.select("min_Y").first().getInt(0)
    val max_Y : Int= minMaxOfColumns.select("max_Y").first().getInt(0)
    val min_dow_x : Double= minMaxOfColumns.select("min_dow_x").first().getDouble(0)
    val max_dow_x : Double= minMaxOfColumns.select("max_dow_x").first().getDouble(0)
    val min_dow_y : Double= minMaxOfColumns.select("min_dow_y").first().getDouble(0)
    val max_dow_y : Double= minMaxOfColumns.select("max_dow_y").first().getDouble(0)
    
    return (min_X, max_X, min_Y, max_Y, min_dow_x, max_dow_x,min_dow_y, max_dow_y)
  }
  
  /**
   * Data scaled using minMax algorithm. 
   * */
  def scaleData(data :DataFrame, min_X : Int, max_X : Int, min_Y :Int, max_Y : Int, min_dow_x :Double, max_dow_x : Double,min_dow_y : Double , max_dow_y : Double ) : DataFrame = {
       
    val range_X : Int= max_X - min_X
    val range_Y : Int= max_Y - min_Y    
    val range_dow_x : Double= max_dow_x - min_dow_x
    val range_dow_y : Double= max_dow_y - min_dow_y
    val df_scaled = data.withColumn("X_scaled",((col("X") - min_X)/range_X))
                   .withColumn("Y_scaled",((col("Y") - min_Y)/range_Y))
                   .withColumn("dow_x_scaled",((col("dow_x") - min_dow_x)/range_dow_x))
                   .withColumn("dow_y_scaled",((col("dow_y") - min_dow_y)/range_dow_y))
    
    
    return df_scaled
  }
  
 
  
  /**
   * Data scaled back to the original scale using minMax algorithm.
   * Only the centroids are returned. Moreover coordinates of day of the week are transformed to the respective week day.
   * */
  def rescaleData(data :DataFrame, min_X : Int, max_X : Int, min_Y :Int, max_Y : Int, min_dow_x :Double, max_dow_x : Double,min_dow_y : Double , max_dow_y : Double) : DataFrame = {
    val range_X : Int= max_X - min_X
    val range_Y : Int= max_Y - min_Y
    val range_dow_x : Double= max_dow_x - min_dow_x
    val range_dow_y : Double= max_dow_y - min_dow_y 
    
    
    
    val data_rescaled = data
                         .groupBy("prediction")
                         .agg((mean("X_scaled")*range_X + min_X).as("x"),
                             (mean("Y_scaled")*range_Y + min_Y).as("y"),
                             (mean("dow_x_scaled")*range_dow_x +min_dow_x).as("dow_x"),
                             (mean("dow_y_scaled")*range_dow_y +min_dow_y).as("dow_y"))  
                            
    val data_with_dow = data_rescaled.withColumn("dow", when(atan2(col("dow_y"),col("dow_x")) > 0, atan2(col("dow_y"),col("dow_x"))*7/(2*math.Pi)).otherwise(atan2(col("dow_y"),col("dow_x"))*7/(2*math.Pi) +7))
    
    return data_with_dow
  }
  
  /**
   * Run the algorithm with the solution of task 1.
   * The number of clusters used is 100.
   * */  
  def run(spark : SparkSession) = {
    println("-------------Task2 start-----------------------")
    val df = readCSVtoDF(spark)
    val (min_X, max_X, min_Y, max_Y, min_dow_x, max_dow_x,min_dow_y, max_dow_y) = getExtremeValuesFromDF(df)
    
    val df_scaled = scaleData(df, min_X , max_X, min_Y , max_Y, min_dow_x, max_dow_x,min_dow_y, max_dow_y )
    
    
    val clusters = kMeansClustering(df_scaled, 100)
    
    val df_rescaled = rescaleData(clusters, min_X , max_X, min_Y , max_Y, min_dow_x, max_dow_x,min_dow_y, max_dow_y )
    printDFtoCSV(df_rescaled.select("x", "y", "dow"), "results/task2.csv")
    println("-------------Task2 end-----------------------")
  }
}