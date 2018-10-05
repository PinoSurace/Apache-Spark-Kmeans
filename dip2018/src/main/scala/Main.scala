
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

//import org.apache.spark.sql.SQLContext.implicits._
//import org.apache.spark.ml.feature.Imputer

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
  case class Point(x: Double, y: Double, day: Double)
  case class FeaturePoint(feature : Int, point : Point)
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
  //df.show()   
  
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
  
  
  val df_scaled = df.withColumn("X_scaled",((col("X") - min_X)/range_X))
                   .withColumn("Y_scaled",((col("Y") - min_Y)/range_Y))
                   .withColumn("DOW_scaled",((col("dow") - min_DOW)/range_DOW))
  //df_scaled.show()                              
                                
  
  
  
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
    
    
    val rdd_scaled: RDD[Point] = df_scaled
    .select("X_scaled", "Y_scaled", "DOW_scaled")
    .rdd.map( x => Point(x(0).asInstanceOf[Double], x(1).asInstanceOf[Double], x(2).asInstanceOf[Double]))
    rdd_scaled.foreach(println)
    
    /** initialize random centroids */
  def initCentroids(points: RDD[Point], number_of_clusters : Int = 3) : Array[Point] = { 
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
    /**
     * Calculate the euclidean distance between two points
     * */
    def euclidean_distance(point1 : Point, point2: Point) :Double = math.sqrt(math.pow(point1.x -point2.x, 2)+
                                                                              math.pow(point1.y -point2.y, 2)+
                                                                              math.pow(point1.day -point2.day, 2))     
      
    
    
    /**
     * KM algorithm implementation for RDD
     * 
     * */
    def KM(num_clusters : Int): Array[FeaturePoint] = {
      var centroids = rdd_scaled.take(3).zipWithIndex.map{ case (point : Point, feature :Int) => new FeaturePoint(feature, point)}
      var difference : Double= 100
      while( difference > 0.0000000001){
        val distances = rdd_scaled.map( point => (centroids.map( centroid =>  ( centroid.feature, euclidean_distance(centroid.point , point )))
                                                                                  .minBy(_._2)._1, point))
    
        val count = distances.count()
        var new_centroids = distances.reduceByKey((point1, point2) => Point((point1.x + point2.x), (point1.y +point2.y), (point1.day +point2.day)))
                                      .map(tuple => FeaturePoint(tuple._1, Point(tuple._2.x/count, tuple._2.y / count, tuple._2.day/count)))
        centroids.sortBy(_.feature)
        new_centroids.sortBy(_.feature)
        centroids.foreach( centroid => new_centroids.foreach( new_centroid => {
          if(centroid.feature == new_centroid.feature)
            difference += euclidean_distance(centroid.point, new_centroid.point)
        }))      
                
        
      }   
      return centroids
    }                                 
    
    
    
    
  
 
  
   
 
 
  
  
}