package com.project
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import java.io._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.util._
import scala.io.StdIn.readLine

class Kmeans {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .config("spark.default.parallelism", 2)
    .getOrCreate()

  val path = "./src/main/resources/dataMay-31-2017.json"
  val MapPartRDD = sparkSession.sparkContext.wholeTextFiles(path).values
  val rawData = sparkSession.read.json(MapPartRDD)
  val extractedPairs = rawData.withColumn("data", explode(rawData.col("data"))).select("data")
  extractedPairs.createOrReplaceTempView("pairs_view")

  def k_mean: Unit = {
    //"ls".run(true).exitValue()
    val splittedPairs = sparkSession.sql("SELECT cast(data[0] as integer) as x1, cast(data[1] as integer) as y1 FROM pairs_view")
    splittedPairs.createOrReplaceTempView("source_table")
    val len = splittedPairs.count().toInt
    
    var x1 = splittedPairs.toDF().select("x1").rdd.map(r => r(0).asInstanceOf[Int]).collect()
    var y1 = splittedPairs.toDF().select("y1").rdd.map(r => r(0).asInstanceOf[Int]).collect()
    
    var points = ArrayBuffer[Point]()
    for (i <- 0 until len) {
      var pt = new Point()
      pt.x = x1(i)
      pt.y = y1(i)
      points += pt
    }

    //losowe punkty
    println("Wpisz ilosc punktow k: ")
    //val test = readLine()
    //printf("You typed: %s", test)
    println()
    println("K-mean started ...")
    var n = 3
    var k_points = ArrayBuffer[Point]()
    val r = Random
    for (i <- 0 until n) {
      var pt = new Point()
      pt.x = r.nextInt(100)
      pt.y = r.nextInt(100)
      pt.group = i + 1
      k_points += pt
    }
    
    println("K-mean started ...")
    kMeanAlg(points, k_points)
    
    for (i <- 0 until len) {
      println(points(i).x + ", " + points(i).y + " | group: " + points(i).group)
    }
    
  }

  def clearGroup(list: ArrayBuffer[Point]) {
    for (i <- 0 until list.size)
      list(i).group = 0
  }
  
  def distance(p1: Point, p2: Point): Double = {
    var x = Math.pow(p1.x.toDouble - p2.x.toDouble, 2)
    var y = Math.pow(p1.y.toDouble - p2.y.toDouble, 2)
    var dist = Math.sqrt(x + y)
    return dist
  }
  
  def giveGroup(point: Point, k_list: ArrayBuffer[Point]) {
    var d = Map[Double, Point]()
    
    for (i <- 0 until k_list.size) {
      var dist = distance(k_list(i), point)
      
      while(d.contains(dist)) {
        dist = dist + 0.000001
      }
      d += (dist -> k_list(i))
    }
    var min_k = d(d.keys.min)
    point.group = min_k.group
  }
  
  def giveGroupCollection(points_list: ArrayBuffer[Point], k_list: ArrayBuffer[Point]) {
    for (i <- 0 until points_list.size) {
      giveGroup(points_list(i), k_list)
    }
  }
  
  def movePointsK(points_list: ArrayBuffer[Point], k_list: ArrayBuffer[Point]) {
    for (i <- 0 until k_list.size) {
      var points_goup = 1.0
      var sum_x = k_list(i).x
      var sum_y = k_list(i).y
      
      for (j <- 0 until points_list.size) {
        if (points_list(j).group == k_list(i).group) {
          points_goup += 1.0
          sum_x += points_list(j).x
          sum_y += points_list(j).y
        }
      }
      
      k_list(i).x = Math.round(sum_x.toDouble/points_goup).toInt
      k_list(i).y = Math.round(sum_y.toDouble/points_goup).toInt
    }
  }
  
  def kMeanAlg(points: ArrayBuffer[Point], k_points: ArrayBuffer[Point]){
    var changeFlag = true
    
    while (changeFlag) {
      var prev_k_points = k_points.clone
      giveGroupCollection(points, k_points)
      movePointsK(points, k_points)
      
      changeFlag = false
      for (i <- 0 until k_points.size) {
        if (Math.abs(k_points(i).x - prev_k_points(i).x) > 1 || Math.abs(k_points(i).y - prev_k_points(i).y) > 1){
          changeFlag = true
        }
      }
    }
  }
  
  class Point{
    var x = 0
    var y = 0
    var group = 0
  }

}