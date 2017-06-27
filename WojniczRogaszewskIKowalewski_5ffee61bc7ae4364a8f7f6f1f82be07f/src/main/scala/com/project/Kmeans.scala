package com.project
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import java.io._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.util._
import scala.io.StdIn._
import org.jfree.chart._
import org.jfree.data.xy._

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

    //random points
    println("Enter the number of points k: ")
    var n = readInt()
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
    
    //making chart
    val dataset = new DefaultXYDataset
    
      var xbuff=ArrayBuffer[Double]()
      var ybuff=ArrayBuffer[Double]()
    for(i<-0 until n){
      ybuff+=k_points(i).y
      xbuff+=k_points(i).x
    }
    val kx=xbuff.toArray
    val ky=ybuff.toArray
    dataset.addSeries("klastry", Array(kx,ky))
    for (i <- 0 until n) {
      var xbuf = ArrayBuffer[Double]()
      var ybuf = ArrayBuffer[Double]()
      for (j <- 0 until len){
        if (points(j).group == k_points(i).group){
          xbuf += points(j).x
          ybuf += points(j).y
        }
      }
      val x = xbuf.toArray
      val y = ybuf.toArray
      val serie = "Series " + i.toString
      dataset.addSeries(serie, Array(x,y))
    }
    val frame = new ChartFrame(
      "Title",
      ChartFactory.createScatterPlot(
        "Plot",
        "X Label",
        "Y Label",
        dataset,
        org.jfree.chart.plot.PlotOrientation.VERTICAL,
        false,false,false
      )
    )
    frame.pack()
    frame.setVisible(true)
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
  
  def deepCopyArray(list: ArrayBuffer[Point]): ArrayBuffer[Point] = {
    var new_list = ArrayBuffer[Point]()
    for (i <- 0 until list.size) {
      var pt = new Point()
      pt.x = list(i).x
      pt.y = list(i).y
      pt.group = list(i).group
      new_list += pt
    }
    return new_list
  }
  
  def kMeanAlg(points: ArrayBuffer[Point], k_points: ArrayBuffer[Point]){
    var changeFlag = true
    
    while (changeFlag) {
      val prev_k_points = deepCopyArray(k_points)
      clearGroup(points)
      //println(prev_k_points(0).x)
      //println(prev_k_points(0).y)
      giveGroupCollection(points, k_points)
      movePointsK(points, k_points)
      //println(prev_k_points(0).x)
      //println(prev_k_points(0).y)
      changeFlag = false
      for (i <- 0 until k_points.size) {
        if (Math.abs(k_points(i).x - prev_k_points(i).x) > 0 || Math.abs(k_points(i).y - prev_k_points(i).y) > 0){
          changeFlag = true
        }
      }
      println("test")
    }
  }
  
  class Point{
    var x = 0
    var y = 0
    var group = 0
  }

}