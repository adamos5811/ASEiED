package com.kgk

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions.col
import scala.io.Source


class IMF {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .getOrCreate()



  def extractData: Unit = {

    val source: String = Source.fromFile("./src/main/resources/dane.json").getLines.mkString
    val json = scala.util.parsing.json.JSON.parseFull(source)
    val mapa = json.get.asInstanceOf[Map[String, List[List[String]]]]

    var SCALA_JSON:String = ""
    var a =0
    for(a <- 0 to mapa("data").length-1){
        SCALA_JSON = SCALA_JSON + """ {"""" + mapa("cols")(0) +"""":"""" + mapa("data")(a)(0) + """",""" +
                                        """"""" + mapa("cols")(1) +"""":"""" + mapa("data")(a)(1) + """",""" +
                                        """"""" + mapa("cols")(2) +"""":"""" + mapa("data")(a)(2) + """",""" +
                                        """"""" + mapa("cols")(3) +"""":"""" + mapa("data")(a)(3) + """"}""" + "\n"
    }


    scala.tools.nsc.io.File("./src/main/resources/dane2.json").writeAll(SCALA_JSON)


        val source2 = sparkSession.read.format("json").load("./src/main/resources/dane2.json")
        val source2_table = source2.collect().toSeq
        val source2_table2 = (for (i <- source2_table) yield (i(2).toString + "|" + i(0).toString + "|" + i(1).toString + "|" + i(3).toString)).mkString("\n")
        scala.tools.nsc.io.File("./src/main/resources/dane_tabela.txt").writeAll(source2_table2)
        //source2.show()

        source2.createOrReplaceTempView("view2")
        val miasta = sparkSession.sql("Select city, count(city) from view2 group by city order by count(city) desc")
        val miasta_table = miasta.collect().toSeq
        val miasta_table2 = (for (i <- miasta_table) yield (i(0).toString + "|" + i(1).toString)).mkString("\n")
        scala.tools.nsc.io.File("./src/main/resources/miasta_tabela.txt").writeAll(miasta_table2)
        //miasta.show()
        val imiona = sparkSession.sql("Select name, count(name) from view2 group by name order by count(name) desc")
        val imiona_table = imiona.collect().toSeq
        val imiona_table2 = (for (i <- imiona_table) yield (i(0).toString + "|" + i(1).toString)).mkString("\n")
        scala.tools.nsc.io.File("./src/main/resources/imiona_tabela.txt").writeAll(imiona_table2)
        //imiona.show()
        val firmy = sparkSession.sql("Select company, count(company) from view2 group by company order by count(company) desc")
        val firmy_table = firmy.collect().toSeq
        val firmy_table2 = (for (i <- firmy_table) yield (i(0).toString + "|" + i(1).toString)).mkString("\n")
        scala.tools.nsc.io.File("./src/main/resources/firmy_tabela.txt").writeAll(firmy_table2)
        //firmy.show()
        
        val miasta2 = sparkSession.sql("Select city,name,count(name) from view2 group by name,city order by count(name) desc")
        //miasta2.show()

        val tabela = miasta2.collect().toSeq
        val tabela2 = for (i <- tabela) yield (i(0).toString + "|" + i(1).toString + "|" + i(2).asInstanceOf[Long])
        val tabela3 = tabela2.mkString("\n")
        //print(tabela3)
        scala.tools.nsc.io.File("./src/main/resources/wynik.txt").writeAll(tabela3)
  }


}