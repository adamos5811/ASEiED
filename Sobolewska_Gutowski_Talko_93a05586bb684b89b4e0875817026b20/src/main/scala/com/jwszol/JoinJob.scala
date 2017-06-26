package com.jwszol


import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}

import org.apache.spark.mllib.util.MLUtils.loadLibSVMFile
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
import java.awt.geom._
import java.awt.image.BufferedImage
import javax.swing.{ImageIcon, JOptionPane}




object NaiveBayesExample {

  def main(args: Array[String]): Unit = {

   


    val conf = new SparkConf().setMaster("local[2]") 
                    .setAppName("NaiveBayes")
    val sc = new SparkContext(conf)

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
    var redPoints: ArrayBuffer[Point] = new ArrayBuffer[Point]()
    var pktx1 = punkty1x.collect()
    var pktx2 = punkty2x.collect()
    var pkty1 = punkty1y.collect()
    var pkty2 = punkty2y.collect()
    
    for(i <- 0 to pktx1.length - 1)
    {
        var bluePoint = new Point(pktx1(i).getInt(0), pkty1(i).getInt(0), Color.BLUE) //dodajemy kazdy punkt do ArrayBuffer
        var redPoint = new Point(pktx2(i).getInt(0), pkty2(i).getInt(0), Color.BLUE)
        bluePoints += bluePoint
        redPoints += redPoint
    }

    
    // Split data into training (60%) and test (40%).

    val Array(training, test) = data.randomSplit(Array(0.6, 0.4))

    sc.stop()

  }

}

