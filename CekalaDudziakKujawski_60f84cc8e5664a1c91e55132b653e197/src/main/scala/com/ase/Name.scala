package com.ase

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col

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
	//joinDs.show()
	sparkSession.sql("select distinct name, count(name) / round(sqrt(count(name))) from names group by name").show()

  }

}

