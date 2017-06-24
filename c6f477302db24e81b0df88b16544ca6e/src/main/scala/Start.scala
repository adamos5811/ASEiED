import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

class Start {
  val sparkSession: SparkSession = SparkSession.builder.
    master("local")
    .appName("spark project")
    .getOrCreate()

  def getData : Unit = {
    val file = "./src/main/resources/dataMay-31-2017.json"
    val parsed = sparkSession.sparkContext.wholeTextFiles(file).values
    val data = sparkSession.read.json(parsed)
    val points = data.withColumn("data", explode(data.col("data"))).select("data")
    val array = new Array[Point](points.count().asInstanceOf[Int])
    var i = 0
    points.foreach { value => var temp = value.apply(0).asInstanceOf[mutable.WrappedArray[Long]]
      array.update(i, Point(temp.apply(0), temp.apply(1), temp.apply(2), temp.apply(3))) // nie podmienia wartości
      i += 1;
    }
    array.foreach(println)
    val team1_apriori = array.length.toFloat / (2 * array.length)
    val team2_apriori = array.length.toFloat / (2 * array.length)
    val target = Target_point(30, 30)
    val radius = 15
    var team1 = 0
    var team2 = 0
    array.foreach { item => if(check_blue(target, radius, item)) team1 += 1}
    array.foreach { item => if(check_red(target, radius, item)) team2 += 1}
    val team1_postpriori = team1 / array.length.toFloat
    val team2_postpriori = team2 / array.length.toFloat
    val prawd_team1 = team1_apriori * team1_postpriori
    val prawd_team2 = team2_apriori * team2_postpriori
    println("Prawdopodobienstwo nr 1 : " + prawd_team1 * 100 + " %")
    println("Prawdopodobienstwo nr 2 : " + prawd_team2 * 100 + " %")
  }

  def check_blue (target:Target_point, radius:Int, item:Point) : Boolean = {
    val punkt = Target_point(item.x1, item.y1)
    if(Math.sqrt(pow(punkt.x - target.x) + pow(punkt.y - target.y)) < radius)
      true
    else
      false
  }

  def check_red (target:Target_point, radius:Int, item:Point) : Boolean = {
    val punkt = Target_point(item.x2, item.y2)
    if(Math.sqrt(pow(punkt.x - target.x) + pow(punkt.y - target.y)) < radius)
      true
    else
      false
  }

  def pow(l : Long) : Long = {
    l * l
  }
}
