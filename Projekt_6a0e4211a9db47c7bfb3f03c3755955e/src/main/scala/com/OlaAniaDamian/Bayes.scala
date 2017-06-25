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
    println(source)
    println("\nRemove tabulacje:")
    val source2 = source.toString.replaceAll("\t", "")
    println(source2)
    println("\nRemove \" :")
    val source3 = source2.toString.replaceAll("\"", "")
    println(source3)
    println("\nRemove space :")
    val source4 = source3.replaceAll(" ", "")
    println(source4)
    val source5 = source4.toString.split("{").mkString("")
    println("SPlit")
    print(source5)
    
    
    
//    val source4 = source3.toString.split(' ')
//    source4.foreach(println)
    
    
    val parsing = parseFull(source)//.get
    println("Parsing:")
    println(parsing)
    val parsing2 = parsing.get
    println("Parsing2:")
    println(parsing2)

    
    println("-----Przyklad-----")
    val capitals = Map("France" -> "Paris", "Japan" -> "Tokyo")
    println(capitals)
    println("capitals.get( \"France\" ) : " +  capitals.get( "France" ))
    println("capitals.get( \"India\" ) : " +  capitals.get( "India" ))
    //val parsing2 = ms get data
  }
}