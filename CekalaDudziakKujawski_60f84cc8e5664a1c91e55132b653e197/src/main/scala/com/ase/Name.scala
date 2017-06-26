package com.ase

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import scalax.chart.api._
import scalax.chart.module.Charting

class Name {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("Project")
    .getOrCreate()

  def joinData: Unit = {
  
	val df = sparkSession.read.json("./src/main/resources/dataMay-31-2017_corrected.json")
	df.createOrReplaceTempView("users_view")
	val df2 = sparkSession.sql("select name as name2, id as id2, city as city2, company as company2 from users_view")
	val joinDs = df.join(df2, df.col("name").equalTo(df2.col("name2")) && df.col("id").notEqual(df2.col("id2")), "left")
	joinDs.createOrReplaceTempView("names")
	val people = sparkSession.sql("select distinct name, (count(name) / round(sqrt(count(name)))) as amount from names group by name")
	val data0 = people.collect().toSeq
	val data1 = for (i <- data0) yield (i(0).toString, i(1).asInstanceOf[Double])
    val chart = PieChart(data1)
    chart.saveAsPNG("./src/main/resources/chart.png")
    chart.show()
    Thread.sleep(5000)
  
  }
}

