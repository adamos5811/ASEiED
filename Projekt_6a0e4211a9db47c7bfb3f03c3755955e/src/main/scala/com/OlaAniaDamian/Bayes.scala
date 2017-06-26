package com.OlaAniaDamian

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import scala.collection.mutable.MutableList
import scala.io.Source
import scala.util.parsing.json.JSON._
import scala.math._
import scalax.chart.api._
import scalax.chart.module.Charting
class Bayes {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .getOrCreate()


  def bayesData(): Unit = {
    
    val source: String = Source.fromFile("./src/main/resources/dane.json").getLines.mkString
    val source2 = source.replaceAll("\t", "")
    val source3 = source2.replaceAll("\"", "")
    val source4 = source3.replaceAll(" ", "")
    val source5 = source4.replaceAll("\\[", "")
    val source6 = source5.split(":")(2)
    val source7 = source6.replaceAll("\\]","")
    val source8 = source7.replaceAll("\\{","")
    val source9 = source8.replaceAll("\\}","")
    val data = source9.split(",")
    println("Dane:")
    println(data.mkString(","))
    val x1 = MutableList[Int]()
    val y1 = MutableList[Int]()
    val x2 = MutableList[Int]()
    val y2 = MutableList[Int]()
    for(i <- 0 until data.length-3 by 4){
      x1 += data(i).toInt
      y1 += data(i+1).toInt
      x2 += data(i+2).toInt
      y2 += data(i+3).toInt
    }
    print("\nx1: ")
    print(x1.mkString(", "))
    print("\ny1: ")
    print(y1.mkString(", "))
    print("\nx2: ")
    print(x2.mkString(", "))
    print("\ny2: ")
    println(y2.mkString(", "))
    
    val closestRed = closestPoints(x1, y1)
    val closestGreen = closestPoints(x2, y2)
    val (newPointGreen, newPointRed) = Bayes(closestGreen.length, closestRed.length, x1.length, x2.length)
    println(newPointGreen, newPointRed)
    

    val data1 = for (i <- 1 to 5) yield (i,i)
    val chart = XYLineChart(data1)
    chart.plot.setRenderer(new org.jfree.chart.renderer.xy.XYLineAndShapeRenderer(false, true))
    chart.saveAsPNG("./src/main/resources/chart.png")
    chart.show()
    Thread.sleep(5000)

    
  }
  
  def closestPoints(x: MutableList[Int], y: MutableList[Int]): MutableList[Int] ={
    val newX = 15
    val newY = 30
    val r = 22.5
    var x_distance = 0
    var y_distance = 0
    var distance = 0.0
    var closestList = new MutableList[Int]()
    for(i <- x; j <- y){
      x_distance = i - newX
      y_distance = j - newY
      distance = sqrt(pow(x_distance, 2) + pow(y_distance, 2))
      if (distance < r)
        closestList += i
    }
    
    return closestList
  }
  
  def Bayes(closestGreen: Int, closestRed: Int, amountRed: Int, amountGreen: Int): (Boolean, Boolean) = {
    var newPointGreen = false
    var newPointRed = false
    val probGreen = closestGreen.toDouble / amountGreen.toDouble
    val probRed = closestRed.toDouble / amountRed.toDouble
    if (probGreen > probRed)
      newPointGreen = true
    else
      newPointRed = true
  
    return (newPointGreen, newPointRed)
    
  }
}