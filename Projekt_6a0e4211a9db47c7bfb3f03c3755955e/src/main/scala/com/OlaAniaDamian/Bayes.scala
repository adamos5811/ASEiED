package com.OlaAniaDamian

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import java.io.File
import java.io.FileInputStream
import org.json4s.jackson.Json
import scala.io.Source
import scala.util.parsing.json.JSON._


class Bayes {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .getOrCreate()


  def bayesData {
    val stream = new FileInputStream("./src/main/resources/dane.json")
    val source: String = Source.fromFile("./src/main/resources/dane.json").getLines.mkString
    println("\nSource:")
    val source2 = source.toString.replaceAll("\t", "")
    val source3 = source2.replaceAll("\"", "")
    val source4 = source3.replaceAll(" ", "")
    val source5 = source4.replaceAll("\\[", "")
    val source6 = source5.split(":")(2)
    val source7 = source6.replaceAll("\\]","")
    val source8 = source7.replaceAll("\\{","")
    val source9 = source8.replaceAll("\\{","")
    println(source9)
    val data = source9.split(",")
    println("Dane:")
    data.foreach(print)
    val x1 = Array[Int]()
    val y1 = Array[Int]()
    val x2 = Array[Int]()
    val y2 = Array[Int]()
    for(i <- 0 to data.length){
      x1 :+ data(i)
      y1 :+ data(i+1)
      x2 :+ data(i+2)
      y2 :+ data(i+3)
      i += 2
    }
    
    
//    val source4 = source3.toString.split(' ')
//    source4.foreach(println)
    
    
    val parsing = parseFull(source)//.get
    println("Parsing:")
    println(parsing)
    val parsing2 = parsing.get
    println("Parsing2:")
    
    
    println("")
    println("-----Przyklad-----")
    val capitals = Map("France" -> "Paris", "Japan" -> "Tokyo")
    println(capitals)
    println("capitals.get( \"France\" ) : " +  capitals.get( "France" ))
    println("capitals.get( \"India\" ) : " +  capitals.get( "India" ))
    //val parsing2 = ms get data
  }
}