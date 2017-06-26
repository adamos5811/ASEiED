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
    val newX = 15
    val newY = 30
    val closestRed = closestPoints(x1, y1, newX, newY)
    val closestGreen = closestPoints(x2, y2,  newX, newY)
    val (newPointGreen, newPointRed) = Bayes(closestGreen.length, closestRed.length, x1.length, x2.length)
    println(newPointGreen, newPointRed)
    if(newPointRed) {
        x1+=newX
        y1+=newY
    }
    else {
      x2+=newX
      y2+=newY
    }    
    val series = new XYSeries("Class 1")
        for ((a,b) <- x1 zip y1) {
          swing.Swing onEDT {
          series.add(a,b)
        }
    }
    val series2 = new XYSeries("Class 2")
        for ((a,b) <- x2 zip y2) {
          swing.Swing onEDT {
          series2.add(a,b)
        }
    }
  
    val SeriesColl = new XYSeriesCollection()
    SeriesColl.addSeries(series)
    SeriesColl.addSeries(series2)
    val chart1 = XYLineChart(SeriesColl)
    chart1.plot.setRenderer(new org.jfree.chart.renderer.xy.XYLineAndShapeRenderer(false, true))
    chart1.show()
    Thread.sleep(50000)
  }
  
  def closestPoints(x: MutableList[Int], y: MutableList[Int], newX: Int, newY: Int): MutableList[Int] ={
    val r = 18.5
    var x_distance = 0
    var y_distance = 0
    var distance = 0.0
    var closestList = new MutableList[Int]()
    for ((i, j) <- (x zip y)){
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
    val allPoints = amountRed + amountGreen
    val aprioriGreen = amountGreen.toDouble / allPoints.toDouble
    val aprioriRed = amountRed.toDouble / allPoints.toDouble
    val probGreen = closestGreen.toDouble / amountGreen.toDouble
    val probRed = closestRed.toDouble / amountRed.toDouble
    val aposterioriGreen = aprioriGreen * probGreen
    val aposterioriRed = aprioriRed * probRed

    if (aposterioriGreen > aposterioriRed)
      newPointGreen = true
    else
      newPointRed = true
  
    return (newPointGreen, newPointRed)
    
  }
}