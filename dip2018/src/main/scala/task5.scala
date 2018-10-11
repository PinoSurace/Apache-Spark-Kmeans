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

case class Point(x: Double, y: Double, xDay: Double, yDay: Double)
case class FeaturePoint(feature : Int, point : Point)
case class RescaleInfo(minX : Double, maxX : Double, minY : Double, maxY : Double)

object task5 {
    /** number of clusters */
  def default_num_clusters = 32
  
  /** number of interations */
  def max_iterations = 200
  
  /** convergence condition */
  def convergence_condition : Double = 1.5D
  
  /** read data and process */
  def readData(spark : SparkSession) : RDD[Point] = {
    val lines = spark.sparkContext.textFile("data/tieliikenneonnettomuudet_2015_onnettomuus.csv")
    val withoutHeader = lines.mapPartitionsWithIndex((i, it) => if (i==0) it.drop(1) else it)
    val dropBlankLine = withoutHeader.filter(x => !x.split(";")(55).isEmpty) // drop the line where x is None
    val processed_data = dropBlankLine.map(line => {
      val splitted = line.split(";")
      if (splitted(19) == "Maanantai") Point(splitted(55).toDouble, splitted(56).toDouble, math.cos(1.0 * 2*math.Pi/ 7) : Double, math.sin(1.0 * 2*math.Pi/ 7) : Double)
      else if (splitted(19) == "Tiistai") Point(splitted(55).toDouble, splitted(56).toDouble, math.cos(2.0*2* math.Pi/ 7) : Double, math.sin(2.0 * 2*math.Pi/ 7) : Double)
      else if (splitted(19) == "Keskiviikko") Point(splitted(55).toDouble, splitted(56).toDouble, math.cos(3.0*2* math.Pi/ 7) : Double, math.sin(3.0 * 2*math.Pi/ 7) : Double)
      else if (splitted(19) == "Torstai") Point(splitted(55).toDouble, splitted(56).toDouble, math.cos(4.0*2* math.Pi/ 7) : Double, math.sin(4.0 * 2*math.Pi/ 7) : Double)
      else if (splitted(19) == "Perjantai") Point(splitted(55).toDouble, splitted(56).toDouble, math.cos(5.0*2* math.Pi/ 7) : Double, math.sin(5.0 * 2*math.Pi/ 7) : Double)
      else if (splitted(19) == "Lauantai") Point(splitted(55).toDouble, splitted(56).toDouble, math.cos(6.0*2* math.Pi/ 7) : Double, math.sin(6.0 * 2*math.Pi/ 7) : Double)
      else Point(splitted(55).toDouble, splitted(56).toDouble, math.cos(7.0* 2*math.Pi/ 7) : Double, 0 : Double)
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
      Point(scaled_x, scaled_y, aPoint.xDay, aPoint.yDay)
    })
    return (scaledPoints, rescale_info)
  }
    
  /** initialize random centroids */
  def initCentroids(points: RDD[Point], number_of_clusters : Int = default_num_clusters) : Array[Point] = { 
    val randomCentroids =  points.takeSample(false, number_of_clusters) 
    
    /*val pointsWithIndex = points.zipWithIndex().map{case (value, index) => (index, value)} // give indexes to the Points
    var randomCentroids = Array[Point]()
    
    val r = scala.util.Random
    for (i <- 0 until number_of_clusters) {
      val randomIdx = r.nextInt(points.count().toInt)
      val randomPoint = pointsWithIndex.lookup(randomIdx).head
      randomCentroids :+= randomPoint
    }*/
    
    return randomCentroids
  }
  
  /** Find Euclidean Distance between 2 points */
  def euclideanDistance(point1 : Point, point2 : Point) : Double = {
    val distance = math.sqrt(math.pow(point1.x - point2.x, 2) 
        + math.pow(point1.y - point2.y, 2)
        + math.pow(point1.xDay - point2.xDay, 2)
        + math.pow(point1.yDay - point2.yDay, 2))  
    return distance
  }
  
  /** Find the index of the closest centroids for each point */
  def findClosestCentroid(point: Point, centroids: Array[Point]) : Int = {
    
    val bestIndex = centroids.zipWithIndex.map(centroid => (centroid._2, euclideanDistance(point, centroid._1))).minBy(_._2)._1
    /*var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centroids.length) {
      val tempDist = euclideanDistance(point, centroids(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }*/
    return bestIndex
  }
  
  /** update the new centroids based on the points that are just clustered,
   *  also compute the sum of square error for each cluster */
  def updateCentroid(aCluster: Iterable[Point]) : (Point, Double) = {
    val averaged_x = aCluster.map(_.x).sum / aCluster.size
    val averaged_y = aCluster.map(_.y).sum / aCluster.size
    val averaged_xday = aCluster.map(_.xDay).sum / aCluster.size
    val averaged_yday = aCluster.map(_.yDay).sum / aCluster.size
    val new_centroid = Point(averaged_x, averaged_y, averaged_xday, averaged_yday)
    // find sum of square error for each cluster
    val cost_x = aCluster.map(aPoint => math.pow(aPoint.x - averaged_x, 2)).sum
    val cost_y = aCluster.map(aPoint => math.pow(aPoint.y - averaged_y, 2)).sum
    val cost_xday = aCluster.map(aPoint => math.pow(aPoint.xDay - averaged_xday, 2)).sum
    val cost_yday = aCluster.map(aPoint => math.pow(aPoint.yDay - averaged_yday, 2)).sum
    val cost_cluster = cost_x + cost_y + cost_xday + cost_yday
    
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
      Point(org_x, org_y, p.xDay, p.yDay)
    })
    return org_centroids 
  }
  
  /** rescale all the points back to original coordinate (to plot later) */
  def rescaling_points(clustered_points: RDD[(Int, Point)], rescaleInfo: RescaleInfo) : RDD[(Int, Point)] = {
//    var res = Array[(Int, Point)]()
    val org_points = clustered_points.map(point => {
      val org_x = point._2.x * (rescaleInfo.maxX - rescaleInfo.minX) + rescaleInfo.minX
      val org_y = point._2.y * (rescaleInfo.maxY - rescaleInfo.minY) + rescaleInfo.minY
      (point._1, Point(org_x, org_y, point._2.xDay, point._2.yDay))
    })
    return org_points
  }
  
  /** export centroids to CSV */
  def toCSV_centroids(centroids: Array[Point], filename: String) {
    val header = Array("X", "Y", "Day")
    val rows = centroids.map(p => {
      val arctan = math.atan2(p.yDay, p.xDay)*7/(2*math.Pi)
      val converted = if(arctan > 0)  arctan else arctan+7
      Array(p.x.toString(), p.y.toString(), converted)
    })
    val allRows = header +: rows
    val csv = allRows.map(_.mkString(",")).mkString("\n")
    new PrintWriter(filename) {write(csv); close()}
  }
  
  /** export clustered points to CSV */
  def toCSV_points(points: RDD[(Int, Point)], filename: String) {
    val header = Array("Cluster", "X", "Y", "Day")
    val rows = points.map(point => {
      val arctan = math.atan2(point._2.yDay, point._2.xDay)*7/(2*math.Pi)
      val converted = if(arctan > 0)  arctan else arctan+7
      Array(point._1.toString(), point._2.x.toString(), point._2.y.toString(), converted)
    }).collect()
    val allRows = header +: rows
    val csv = allRows.map(_.mkString(",")).mkString("\n")
    new PrintWriter(filename) {write(csv); close()}
  }
  
  
  def run(spark : SparkSession) {
    println("-------------Task05: Read Original Data---------------")
    val data = readData(spark)
    println("-------------Scaled data-----------------")
    val scaled_data = minMaxScaling(data)
    val scaled_points = scaled_data._1
    val rescale_info = scaled_data._2
    println("-------------Random Initialized Centroids-----------------")
    val randomCentroids = initCentroids(scaled_points)
    println("-------------Run K-Means----------------")
    val (scaled_centroids, clustered_points, distortion) = kmeansRun(scaled_points, randomCentroids)
    println("-------------Rescale----------------")
    println("-------------Centroids---------------")
    val rescaled_centroids = rescaling_centroids(scaled_centroids, rescale_info)
    rescaled_centroids.foreach(println)
    println("-------------Points---------------")
    val rescaled_clustered_points = rescaling_points(clustered_points, rescale_info)
    rescaled_clustered_points.take(20).foreach(println)    
    println()
    println("-------------Export to CSV----------------")
    toCSV_centroids(rescaled_centroids, "results/task5.csv")
    //toCSV_points(rescaled_clustered_points, "results/task5_clusteredPoints.csv")
    println("!!!!!!!!!!!!!!!!!!!!Done!!!!!!!!!!!!!!!!!!!")
    
  }
}