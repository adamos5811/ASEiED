package com.jwszol

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import scala.collection.mutable.ArrayBuffer

import java.awt._
import scalax.chart.api._

import scala.util.Random
import org.apache.log4j.{Level, Logger}

object NaiveBayesExample {
    

    

    def consoleCleaner: Unit = { //dzieki temu nie bedziemy widziec za duzo wiadomosci w konsoli
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
    }
  def main(args: Array[String]): Unit = {

    consoleCleaner //usuwamy niepotrzebne wiadomosci

    val conf = new SparkConf().setMaster("local[2]") 
                    .setAppName("NaiveBayes")
    val sc = new SparkContext(conf)

    val sparkSession = SparkSession.builder.getOrCreate()

    val sqlContext = new org.apache.spark.sql.SQLContext(sc) //tworzymy context do operacji sql na danych

    val path = "./src/main/resources/dataMay-31-2017.json" //wczytywanie json
    val MapPartRDD = sc.wholeTextFiles(path).values
    val rawData = sqlContext.read.json(MapPartRDD)
    val extractedData = rawData.withColumn("data", explode(rawData.col("data"))).select("data")
    extractedData.createOrReplaceTempView("pairs_view")
    
    val punkty1x = sqlContext.sql("SELECT cast(data[0] as integer) as x FROM pairs_view") //ladujemy osobno wspolrzedne
    val punkty1y = sqlContext.sql("SELECT cast(data[1] as integer) as y FROM pairs_view")
    val punkty2x = sqlContext.sql("SELECT cast(data[2] as integer) as x FROM pairs_view")
    val punkty2y = sqlContext.sql("SELECT cast(data[3] as integer) as y FROM pairs_view")

    var bluePoints: ArrayBuffer[Point] = new ArrayBuffer[Point]()
    var yellowPoints: ArrayBuffer[Point] = new ArrayBuffer[Point]()
    var pktx1 = punkty1x.collect()
    var pktx2 = punkty2x.collect()
    var pkty1 = punkty1y.collect()
    var pkty2 = punkty2y.collect()
    
    for(i <- 0 to pktx1.length - 1)
    {
        var bluePoint = new Point(pktx1(i).getInt(0), pkty1(i).getInt(0), Color.BLUE) //dodajemy kazdy punkt do ArrayBuffer
        //println("Dodano do niebieskich: " + pktx1(i).getInt(0) + " " +pkty1(i).getInt(0))
        var yellowPoint = new Point(pktx2(i).getInt(0), pkty2(i).getInt(0), Color.YELLOW)
        bluePoints += bluePoint
        yellowPoints += yellowPoint
    }

    var addedPoints: ArrayBuffer[Point] = new ArrayBuffer[Point]()

    val blueRDD = sparkSession.sparkContext.parallelize(bluePoints)
    val yellowRDD = sparkSession.sparkContext.parallelize(yellowPoints)
    
    val numberOfAllPoints = blueRDD.count() + yellowRDD.count()
    val randomGenerator = Random

    val blueApriori = blueRDD.count().toDouble / numberOfAllPoints.toDouble
    val yellowApriori = yellowRDD.count().toDouble / numberOfAllPoints.toDouble

    println("blue " + blueApriori + "     yellow " + yellowApriori)
    //println("blueInR " + blueInR.count() + "     yellowInR " + yellowInR.count())
    println()

    for (i <- 0 to 9) {

        var FIELDSIZE = 7
        var found = 0
        val point = new Point(randomGenerator.nextInt(40), randomGenerator.nextInt(40), Color.PINK)
        var blueInR = blueRDD.filter(p => math.sqrt(math.pow(point.x - p.x, 2) + math.pow(point.y - p.y, 2)) < FIELDSIZE)
        var yellowInR = yellowRDD.filter(p => math.sqrt(math.pow(point.x - p.x, 2) + math.pow(point.y - p.y, 2)) < FIELDSIZE)
        addedPoints += point
        while(found<5)
        {
        blueInR = blueRDD.filter(p => math.sqrt(math.pow(point.x - p.x, 2) + math.pow(point.y - p.y, 2)) < FIELDSIZE)
        yellowInR = yellowRDD.filter(p => math.sqrt(math.pow(point.x - p.x, 2) + math.pow(point.y - p.y, 2)) < FIELDSIZE)
        found = blueInR.count().toInt + yellowInR.count().toInt
        FIELDSIZE+=1
        }
        val blueChance = blueInR.count().toDouble / blueRDD.count().toDouble
        val yellowChance = yellowInR.count().toDouble / yellowRDD.count().toDouble
        
        
        val blueAposteriori = blueApriori * blueChance
        val yellowAposteriori = yellowApriori * yellowChance

        println("blueApost " + blueAposteriori + "     yellowApost " + yellowAposteriori)
        println("blueInR " + blueInR.count() + "     yellowInR " + yellowInR.count())
        println()

        if (blueAposteriori > yellowAposteriori) {
          point.color = Color.BLUE
          bluePoints += point
          println("blue")
          println()
        }
        else if (yellowAposteriori > blueAposteriori) {
          point.color = Color.YELLOW
          yellowPoints += point
          println("yellow")
          println()
        }
    }
    val yellowSeries = new XYSeries("Klasa 1")
    for((myPoint,i) <- yellowPoints.view.zipWithIndex) yellowSeries.add(myPoint.x,myPoint.y )
    
    val blueSeries = new XYSeries("Klasa 2")
    for((myPoint,i) <- bluePoints.view.zipWithIndex) blueSeries.add(myPoint.x,myPoint.y )
    

    val SeriesColl = new XYSeriesCollection()
    SeriesColl.addSeries(yellowSeries)
    SeriesColl.addSeries(blueSeries)
    
    val chart = XYLineChart(SeriesColl)
    
    
    chart.plot.setRenderer(new org.jfree.chart.renderer.xy.XYLineAndShapeRenderer(false, true))
    chart.plot.getRenderer().setSeriesPaint(0, Color.YELLOW)
    chart.plot.getRenderer().setSeriesPaint(1, Color. BLUE)


    chart.show()
    Thread.sleep(500000)

    sc.stop()

  }

}

