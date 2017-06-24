package com.OlaAniaDamian

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import scala.io.Source


class Bayes {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .getOrCreate()


  def bayesData {
    println("siemaneczko")

  }

}

