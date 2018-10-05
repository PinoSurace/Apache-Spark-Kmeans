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

//import org.apache.spark.sql.SQLContext.implicits._
//import org.apache.spark.ml.feature.Imputer
case class Point(x: Double, y: Double, day: Double)
case class FeaturePoint(feature : Int, point : Point)
case class RescaleInfo(minX : Double, maxX : Double, minY : Double, maxY : Double)


/** used for task 5 and 6 */
class Kmeans extends Serializable{
  /** number of clusters */
  def default_num_clusters = 3
  
  /** number of interations */
  def max_iterations = 200
  
  /** convergence condition */
  def convergence_condition : Double = 1.5D
  
  /** read data and process */
  def readData() : RDD[Point] = {
    // Suppress the log messages:
    Logger.getLogger("org").setLevel(Level.OFF)
  
    val spark = SparkSession.builder()
                          .appName("ex2")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()
                          
    val lines = spark.sparkContext.textFile("data/2015.csv")
    val withoutHeader = lines.mapPartitionsWithIndex((i, it) => if (i==0) it.drop(1) else it)
    val dropBlankLine = withoutHeader.filter(x => !x.split(";")(55).isEmpty) // drop the line where x is None
    val processed_data = dropBlankLine.map(line => {
      val splitted = line.split(";")
      if (splitted(19) == "Maanantai") Point(splitted(55).toDouble, splitted(56).toDouble, 1)
      else if (splitted(19) == "Tiistai") Point(splitted(55).toDouble, splitted(56).toDouble, 2)
      else if (splitted(19) == "Keskiviikko") Point(splitted(55).toDouble, splitted(56).toDouble, 3)
      else if (splitted(19) == "Torstai") Point(splitted(55).toDouble, splitted(56).toDouble, 4)
      else if (splitted(19) == "Perjantai") Point(splitted(55).toDouble, splitted(56).toDouble, 5)
      else if (splitted(19) == "Lauantai") Point(splitted(55).toDouble, splitted(56).toDouble, 6)
      else if (splitted(19) == "Sunnuntai") Point(splitted(55).toDouble, splitted(56).toDouble, 7)
      else Point(splitted(55).toDouble, splitted(56).toDouble, 0)
    })
    return processed_data
  }
  
  /** Do data scaling because the 3 columns of the data are not in the same scale */
  def minMaxScaling(points: RDD[Point]) : (RDD[Point], RescaleInfo) = {
    val X_arr = points.map(_.x).collect()
    val minX = X_arr.reduceLeft(_ min _)
    val maxX = X_arr.reduceLeft(_ max _)
    val Y_arr = points.map(_.y).collect()
    val minY = Y_arr.reduceLeft(_ min _)
    val maxY = Y_arr.reduceLeft(_ max _)
    
    val rescale_info = RescaleInfo(minX, maxX, minY, maxY)
    
    val scaledPoints = points.map(aPoint => {
      val scaled_x = (aPoint.x - minX) / (maxX - minX)
      val scaled_y = (aPoint.y - minY) / (maxX - minX)
      val scaled_day = (aPoint.day - 1) / 6   // we only have days from 1 to 7
      Point(scaled_x, scaled_y, scaled_day)
    })
    return (scaledPoints, rescale_info)
  }
    
  /** initialize random centroids */
  def initCentroids(points: RDD[Point], number_of_clusters : Int = default_num_clusters) : Array[Point] = { 
    val pointsWithIndex = points.zipWithIndex().map{case (value, index) => (index, value)} // give indexes to the Points
    var randomCentroids = Array[Point]()
    
    val r = scala.util.Random
    for (i <- 0 until number_of_clusters) {
      val randomIdx = r.nextInt(points.count().toInt)
      val randomPoint = pointsWithIndex.lookup(randomIdx).head
      randomCentroids :+= randomPoint
    }
    
    return randomCentroids
  }
  
  /** Find Euclidean Distance between 2 points */
  def euclideanDistance(point1 : Point, point2 : Point) : Double = {
    val distance = math.sqrt(math.pow(point1.x - point2.x, 2) 
        + math.pow(point1.y - point2.y, 2)
        + math.pow(point1.day - point2.day, 2))  
    return distance
  }
  
  /** Find the index of the closest centroids for each point */
  def findClosestCentroid(point: Point, centroids: Array[Point]) : Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centroids.length) {
      val tempDist = euclideanDistance(point, centroids(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    return bestIndex
  }
  
  /** update the new centroids based on the points that are just clustered,
   *  also compute the sum of square error for each cluster */
  def updateCentroid(aCluster: Iterable[Point]) : (Point, Double) = {
    val averaged_x = aCluster.map(_.x).sum / aCluster.size
    val averaged_y = aCluster.map(_.y).sum / aCluster.size
    val averaged_day = aCluster.map(_.day).sum / aCluster.size
    val new_centroid = Point(averaged_x, averaged_y, averaged_day)
    // find sum of square error for each cluster
    val cost_x = aCluster.map(aPoint => math.pow(aPoint.x - averaged_x, 2)).sum
    val cost_y = aCluster.map(aPoint => math.pow(aPoint.y - averaged_y, 2)).sum
    val cost_day = aCluster.map(aPoint => math.pow(aPoint.x - averaged_day, 2)).sum
    val cost_cluster = cost_x + cost_y + cost_day
    
    return (new_centroid, cost_cluster)
  }
  
  /** Recursively run until converge (use @tailrec to optimize the tail recursive function) */
  @tailrec final def kmeansRun(points: RDD[Point], centroids: Array[Point], iterationNum: Int = 1) : 
                                                                    (Array[Point], RDD[(Int, Point)], Double) = {
    // assign each point a centroid index
    val clustered_points = points.map(aPoint => {
      val idx = findClosestCentroid(aPoint, centroids)
      (idx, aPoint)
    }) // each point now has an index correspond to its centroid (the closest center)
    
    val indexed_centroids_with_error = clustered_points.groupByKey()
                                                       .mapValues(aCluster => updateCentroid(aCluster))
                                               
    val new_centroids = indexed_centroids_with_error.map{ case (key, value) => value._1 }
                                                    .collect()
    
    // find the sum of square error of all clusters (the cost function that we want to minimize with Elbow Algorithm)
    val distortion = indexed_centroids_with_error.map{ case (key, value) => value._2 }.sum                                     
    
                                        
    // now find the total distance between the old and new centroids
    var total_distance = 0.0
    for (i <- 0 until centroids.length) {
      total_distance += euclideanDistance(centroids(i), new_centroids(i))
    }
    // stop if there is no significant changes at the centroid points or we exceed the number of iterations
    if (total_distance < convergence_condition || iterationNum > max_iterations) {
      return (new_centroids, clustered_points, distortion)
    } else {
      kmeansRun(points, new_centroids, iterationNum+1)
    }
  }

  /** rescale the centroids back to original coordinate */
  def rescaling_centroids (scaled_centroids : Array[Point], rescaleInfo : RescaleInfo) : Array[Point] = {
    val org_centroids = scaled_centroids.map(p => {
      val org_x = p.x * (rescaleInfo.maxX - rescaleInfo.minX) + rescaleInfo.minX
      val org_y = p.y * (rescaleInfo.maxY - rescaleInfo.minY) + rescaleInfo.minY
      val org_day = p.day * 6 + 1
      Point(org_x, org_y, org_day)
    })
    return org_centroids
  }
  
  /** rescale all the points back to original coordinate (to plot later) */
  def rescaling_points(clustered_points: RDD[(Int, Point)], rescaleInfo: RescaleInfo) : RDD[(Int, Point)] = {
//    var res = Array[(Int, Point)]()
    val org_points = clustered_points.map(point => {
      val org_x = point._2.x * (rescaleInfo.maxX - rescaleInfo.minX) + rescaleInfo.minX
      val org_y = point._2.y * (rescaleInfo.maxY - rescaleInfo.minY) + rescaleInfo.minY
      val org_day = point._2.day * 6 + 1
      (point._1, Point(org_x, org_y, org_day))
    })
    return org_points
  }
  
  /** export centroids to CSV */
  def toCSV_centroids(centroids: Array[Point], filename: String = "output/task5.csv") {
    val header = Array("X", "Y", "Day")
    val rows = centroids.map(p => {
      Array(p.x.toString(), p.y.toString(), p.day.toString())
    })
    val allRows = header +: rows
    val csv = allRows.map(_.mkString(",")).mkString("\n")
    new PrintWriter(filename) {write(csv); close()}
  }
  
  /** export clustered points to CSV */
  def toCSV_points(points: RDD[(Int, Point)], filename: String = "output/task5_clusteredPoints.csv") {
    val header = Array("Cluster", "X", "Y", "Day")
    val rows = points.map(point => {
      Array(point._1.toString(), point._2.x.toString(), point._2.y.toString(), point._2.day.toString())
    }).collect()
    val allRows = header +: rows
    val csv = allRows.map(_.mkString(",")).mkString("\n")
    new PrintWriter(filename) {write(csv); close()}
  }
  
  /** export Elbow result to CSV */
  def toCSV_elbow(costs : Array[(Int, Double)], filename: String = "output/task6_CostWithDifferentNumberOfClusters.csv") {
    val header = Array("NumOfCluster", "Cost")
    val rows = costs.map(x => Array(x._1.toString(), x._2.toString()))
    val allRows = header +: rows
    val csv = allRows.map(_.mkString(",")).mkString("\n")
    new PrintWriter(filename) {write(csv); close()}
  }
}




object main extends App {
  
  //menu
/*  if (args.length == 0) {
        println("error, parameters missing")
   }
  else if (args(0) == "task"){
    args(1) match {
      case 1 =>
      case 2 =>
      case 3 =>
      case 4 =>
      case 5 =>
      case 6 =>  
      
    }
    
  } */
  
  
  // Suppress the log messages:
  Logger.getLogger("org").setLevel(Level.OFF)

	val spark = SparkSession.builder()
                          .appName("ex2")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", "5")
  
  import spark.implicits._
  
                   
 /**
  * get data from the csv file to dataframe and select only the columns needed: X, Y and Vkpv.
  * Moreover dirty data are romoved.
  * 
  * */
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
                   
  val weekDays = Seq(
  Row("Maanantai" : String, 1.0 : Double),
  Row("Tiistai" : String, 2.0 : Double),
  Row("Keskiviikko" : String,3.0 : Double),
  Row("Torstai" : String,4.0 : Double),
  Row("Perjantai" : String,5.0 : Double),
  Row("Lauantai" : String,6.0 : Double),
  Row("Sunnuntai" : String,7.0 : Double))

  val weekDaysSchema = List(
    StructField("Vkpv", StringType, true),
    StructField("dow", DoubleType, true)
  )

  val weekDaysDF = spark.createDataFrame(
    spark.sparkContext.parallelize(weekDays),
    StructType(weekDaysSchema)
  )  

/**
 * Create a dataframe with coordinates X and Y and integers as weekdays
 * 
 * 
 * */  
  val df : DataFrame= dfXYVkpv.join(weekDaysDF, dfXYVkpv("Vkpv") === weekDaysDF("Vkpv"), "left_outer").drop("Vkpv")
  df.show()   
  
/**
 * Create df with scaled data
 * 
 * */ 
  val minMaxOfColumns = df.agg(min("X").as("min_X"), max("X").as("max_X"), 
                                min("Y").as("min_Y"), max("Y").as("max_Y"))
  
  val min_X  : Int= minMaxOfColumns.select("min_X").first().getInt(0) 
  val max_X : Int= minMaxOfColumns.select("max_X").first().getInt(0) 
  val min_Y : Int= minMaxOfColumns.select("min_Y").first().getInt(0)
  val max_Y : Int= minMaxOfColumns.select("max_Y").first().getInt(0)
  val min_DOW : Double= 1.0
  val max_DOW : Double= 7.0
  val range_X : Int= max_X - min_X
  val range_Y : Int= max_Y - min_Y
  val range_DOW : Double= 6.0
  val rescale_info= RescaleInfo(min_X, max_X, min_Y, max_Y)
  println(rescale_info)
  val df_scaled = df.withColumn("X_scaled",((col("X") - min_X)/range_X))
                   .withColumn("Y_scaled",((col("Y") - min_Y)/range_Y))
                   .withColumn("DOW_scaled",((col("dow") - min_DOW)/range_DOW))
  df_scaled.show()                              
                                
  
  
  
/**
 * Function implements the k means clustering for task 1.
 * */               
                   
def kMeansClusteringXY(n : Int, elbow : Boolean = false) : DataFrame = {
     
    val vectorAssembler = new VectorAssembler().setInputCols(Array("X", "Y")).setOutputCol("features")                              
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
    val coordinates : DataFrame = df.select("X", "Y")                                  
    val pipeLine = transformationPipeline.fit(coordinates)
    val transformedTraining = pipeLine.transform(coordinates)
    val kmeans = new KMeans().setK(n).setSeed(1L)
    val kmModel = kmeans.fit(transformedTraining)
 
    if(elbow)
      return kmModel.summary.predictions
     
     /**
      * Result written in csv file 
      * */     
    kmModel.summary.predictions.select("X", "Y")
    .coalesce(1)
    .write
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .save("results/basic.csv")
    return kmModel.summary.predictions                                          
    
    
    
  }               

  /**
 	* Function implements the k means clustering for task 2.
 	* */  
  def kMeansClusteringXYDOW(n : Int, elbow : Boolean = false) : DataFrame = {
    //task2
  //val weekIndexer = new StringIndexer().setInputCol("Vkpv").setOutputCol("dayOfWeek")
    val vectorAssembler = new VectorAssembler().setInputCols(Array("X", "Y", "dow")).setOutputCol("features") 
    //val transformationPipeline = new Pipeline()
    //                              .setStages(Array(weekIndexer, vectorAssembler))
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler)) 
                                  
    
    
   
    //val data : DataFrame = df.withColumn("dow",df.select("Vkpv").map(x =>returnDayAsInt(x) ))//returnDayAsInt(col("Vkpv")) ) // withColumn("dow", returnDayAsInt("Vkpv"))//.withColumnRenamed("Vkpv", "dayOfWeek")                                 
   val pipeLine = transformationPipeline.fit(df)
   val transformedTraining = pipeLine.transform(df)
 //transformedTraining.show
   val kmeans = new KMeans().setK(n).setSeed(1L)
   val kmModel = kmeans.fit(transformedTraining)
 //kmModel1.summary.predictions.select("features").take(1).foreach(println)
   /**
    * Result written in csv file 
    * */
   if(elbow)
     return kmModel.summary.predictions
   kmModel.summary.predictions.select("X", "Y", "dow").coalesce(1).write.format("com.databricks.spark.csv")
                               .option("header", "true").save("results/task2.csv")
   return kmModel.summary.predictions
    
  }                   
   
 
 //task3
 
 
 //task4
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
    println(toreturn)
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
  /**
   * Function that calcultates the elbow point based on the input (2D or 3D)
   * */
  def elbow(dimension :Int) = {
    val range  = 2 to 200 toList;
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
  println("Elbow 2-------------------------------------")
  //elbow(2)
  println("Elbow 3-------------------------------------")
  //elbow(3)
 
 
  
  
 
 def sumDistances (i : Int) : Double = {
 //for(i <- 1 to 200){
   println(i) 
    val vectorAssembler = new VectorAssembler()
                            .setInputCols(Array("X", "Y", "dow"))
                            .setOutputCol("features") 
    //val transformationPipeline = new Pipeline()
    //                              .setStages(Array(weekIndexer, vectorAssembler))
    val transformationPipeline = new Pipeline()
                                  .setStages(Array(vectorAssembler))                              
 
   val coordinates3d = df.select("Vkpv")
   //val coordinates3d = df.withColumn("dow", udf(returnDayAsInt(col("Vkpv").cast(StringType)))))
   //coordinates3d.show
   
    //val data : DataFrame = df.withColumn("dow",df.select("Vkpv").map(x =>returnDayAsInt(x) ))//returnDayAsInt(col("Vkpv")) ) // withColumn("dow", returnDayAsInt("Vkpv"))//.withColumnRenamed("Vkpv", "dayOfWeek")                                 
   val pipeLine = transformationPipeline.fit(coordinates3d)
   val transformedTraining = pipeLine.transform(coordinates3d)
   val kmeans = new KMeans().setK(i).setSeed(1L)
   val kmModel = kmeans.fit(transformedTraining)
   val pred = kmModel.summary.predictions //.groupBy("prediction").agg(mean("X").alias("avgx"), mean("Y").alias("avgy")).show
   val avg : DataFrame= pred.groupBy("prediction").agg(mean("X").alias("avgx"), mean("Y").alias("avgy"))
   val complete : DataFrame= pred.join(avg, pred("prediction") === avg("prediction"), "left_outer")
   return complete
   .agg(sum((col("X") - col("avgx"))*(col("X") - col("avgx")) + (col("Y") - col("avgy"))*(col("Y") - col("avgy"))).cast("double")).first.getDouble(0)
   
   
   val weights : DataFrame= complete
   .withColumn("w", (col("X") - col("avgx"))*(col("X") - col("avgx")) + (col("Y") - col("avgy"))*(col("Y") - col("avgy")))
   
    return weights.agg(sum("w").alias("total").cast("double")).first.getDouble(0)
   
  
 }
  
  /*val range  = 2 to 200 toList
  val res = range.map(x => sumDistances(x)).zipWithIndex
  val res1 = spark.createDataFrame(res).toDF("dist", "ind")
  
  res1.coalesce(1)
 .write
 .format("com.databricks.spark.csv")
 .option("header", "true")
 .save("data/rrr.csv")*/
  
  
 /* val resSchema = Seq(
  StructField("dist", DoubleType, true),
  StructField("num", IntegerType, true)
  )
  
  //val res1 = spark.sparkContext.parallelize(res).toDF("dist", "ind")
  val res123 = spark.createDataFrame(
  spark.sparkContext.parallelize(res),
  StructType(resSchema)
  )*/
  
  //task 5
 /* def day (data_s : String) : Int = {
    data_s match {
      case MAANANTAI => 1
      case TIISTAI => 2
      case KESKIVIIKKO => 3
      case TORSTAI => 4
      case PERJANTAI => 5
      case LAUANTAI =>  6
      case SUNNUNTAI => 7
    }
  }
  
  val MAANANTAI = "Maanantai"
  val  TIISTAI = "Tiistai"
  val KESKIVIIKKO = "Keskiviikko"
  val TORSTAI = "Torstai"
  val PERJANTAI = "Perjantai"
  val LAUANTAI = "Lauantai"
  val SUNNUNTAI = "Sunnuntai"*/
 /* 
  val someData = Seq(
  Row("Maanantai" : String, 1.0 : Double),
  Row("Tiistai" : String, 2.0 : Double),
  Row("Keskiviikko" : String,3.0 : Double),
  Row("Torstai" : String,4.0 : Double),
  Row("Perjantai" : String,5.0 : Double),
  Row("Lauantai" : String,6.0 : Double),
  Row("Sunnuntai" : String,7.0 : Double)
)

val someSchema = List(
  StructField("weekDay", StringType, true),
  StructField("num", DoubleType, true)
)

val someDF = spark.createDataFrame(
  spark.sparkContext.parallelize(someData),
  StructType(someSchema)
)



  val df5 : DataFrame= df.join(someDF, df("Vkpv") === someDF("weekDay"), "left_outer")*/
  //val df4 : DataFrame= df5.select("X", "Y", "num").na.drop
  
  
  /*
 
 def kMeansClusteringRDD(n : Int, elbow : Boolean = false) {
    val rows: RDD[Row] = df.rdd
    val points = rows.take(n).zipWithIndex
    
    val d = rows.map(x => ((points.map( y => ( y._2,
                        math.pow(x(0).asInstanceOf[Int] - y._1(0).asInstanceOf[Int],2) +
                        math.pow(x(1).asInstanceOf[Int] - y._1(1).asInstanceOf[Int],2) ) 
                        //math.pow(x.getInt(3) - points(1).getInt(3),2)
                          
    ).minBy(_._2)._1), (x(0).asInstanceOf[Int], x(1).asInstanceOf[Int], 1)))
  
  val new_points = d.reduceByKey((x,y) => (x._1 + y._1, x._2 +y._2, x._3+y._3)).map(x =>(x._1, x._2._1/x._2._3, x._2._2/x._2._3))
  new_points.foreach(println)
                        
    
    
  }*/
 
 /*def day (data_s : String) : Int = {
    return 1
  }
  val df4 : DataFrame= weekDaysDF.select("X", "Y").na.drop
  val rows: RDD[Row] = df4.rdd

   
  val num : Int = 3
  
  val points = rows.take(3).zipWithIndex
  points.foreach(println)
  */
  /*val d = rows.map(x => math.pow(x(0).asInstanceOf[Int] - points(1)(0).asInstanceOf[Int],2) +
                        math.pow(x(1).asInstanceOf[Int] - points(1)(1).asInstanceOf[Int],2)  
                        //math.pow(x.getInt(3) - points(1).getInt(3),2)
                          
  )
  d.foreach(println)  */// funziona
 /* 
  val d = rows.map(x => ((points.map( y => ( y._2,
                        math.pow(x(0).asInstanceOf[Int] - y._1(0).asInstanceOf[Int],2) +
                        math.pow(x(1).asInstanceOf[Int] - y._1(1).asInstanceOf[Int],2) ) 
                        //math.pow(x.getInt(3) - points(1).getInt(3),2)
                          
  ).minBy(_._2)._1), (x(0).asInstanceOf[Int], x(1).asInstanceOf[Int], 1)))
  //d.foreach(println)
  
  val new_points = d.reduceByKey((x,y) => (x._1 + y._1, x._2 +y._2, x._3+y._3)).map(x =>(x._1, x._2._1/x._2._3, x._2._2/x._2._3))
  new_points.foreach(println)
  
  */
  
  //se sono uguali allora mappo
  
  
  
  
  //task 6 // aggrego anche i punti oltre alla 
  
  //val elbow_dist =  newpoints.reduce((x,y) =>)
  
  
  
  //if (points == new_points)
  
  //val avg_points = 
  
  /*val dist = rows.map( x=> points.map( i => (x(0).asInstanceOf[Int] - i(0).asInstanceOf[Int])*(x(0).asInstanceOf[Int] - i(0).asInstanceOf[Int]) 
      + (x(1).asInstanceOf[Int] - i(1).asInstanceOf[Int])*(x(1).asInstanceOf[Int] - i(1).asInstanceOf[Int])
      + (x(2).asInstanceOf[Int] - i(2).asInstanceOf[Int])*(x(2).asInstanceOf[Int] - i(2).asInstanceOf[Int])
      ).min)
      
  dist.foreach(println)    */
  //dist.foreach(println)
  
  
  /**
   * FROM HERE YOU CAN START!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
   * 
   * 
   * 
   * */
    
    println("----------------------------Task 5 and 6---------------------------------------")
    val scaled_points: RDD[Point] = df_scaled
    .select("X_scaled", "Y_scaled", "DOW_scaled")
    .rdd.map( x => Point(x(0).asInstanceOf[Double], x(1).asInstanceOf[Double], x(2).asInstanceOf[Double]))
//    scaled_points.foreach(println)


    
    // task 05
    val kmean = new Kmeans()
    val randomCentroids = kmean.initCentroids(scaled_points)
    val (scaled_centroids, clustered_points, distortion) = kmean.kmeansRun(scaled_points, randomCentroids)
    val rescaled_centroids = kmean.rescaling_centroids(scaled_centroids, rescale_info)
    val rescaled_clustered_points = kmean.rescaling_points(clustered_points, rescale_info)
    kmean.toCSV_centroids(rescaled_centroids, "results/task5.csv")
    kmean.toCSV_points(rescaled_clustered_points, "results/task5_clusteredPoints.csv")
    println("Task 05 Done!!!!!!!!")
    
    // task 06
    var cost_function_values = Array[(Int,Double)]()
    for (i <- 2 until 20) {
      val randomCentroids = kmean.initCentroids(scaled_points, i)
      val (scaled_centroids, clustered_points, distortion) = kmean.kmeansRun(scaled_points, randomCentroids) 
      cost_function_values :+= (i, distortion)
      println("porco dio!!")
    }
    cost_function_values.foreach(println)
    kmean.toCSV_elbow(cost_function_values, "results/task6.csv")
    println("Task 06 Done!!!!!!!!")
}
