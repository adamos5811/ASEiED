package com.ase

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col

class Name {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("Project")
    .getOrCreate()


  def joinData: Unit = {
	val df = sparkSession.read.json("./src/main/resources/dataMay-31-2017.json")
	df.show()
  }

}

