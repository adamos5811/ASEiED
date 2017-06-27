package com.ts
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import java.io._
import scala.collection.mutable.ArrayBuffer

class SortingJob {

  val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .config("spark.default.parallelism", 2)
    .getOrCreate()

  val path = "./src/main/resources/dataMay-31-2017.json"
  val srcRDD = sparkSession.sparkContext.wholeTextFiles(path).values
  val rawData = sparkSession.read.json(srcRDD)
  val extrPairs = rawData.withColumn("data", explode(rawData.col("data"))).select("data")
  extrPairs.createOrReplaceTempView("pairs_view")

  
  def selSort(a: ArrayBuffer[InputData]){
    def swap(i: Int, j: Int){
        val t = a(i)
          a(i) = a(j)
          a(j) = t
        }

    def minimal(i: Int){
        for(j <- i until a.length){
            if(a(j).value < a(i).value) swap(i, j)
            }
        }
    
    def sorting(){
        for(i <- 0 until a.length){
            minimal(i)
            }
        }
    sorting()
    }

  def selectionSort: Unit = {
    
    val t_start = System.currentTimeMillis()

    val splittedPairs = sparkSession.sql("SELECT cast(data[0] as integer) as id, cast(data[1] as float) as value FROM pairs_view")
    splittedPairs.createOrReplaceTempView("source_table")
    val length = splittedPairs.count().toInt
    
    var result = splittedPairs.toDF().select("value").rdd.map(r => r(0).asInstanceOf[Float]).collect()
    var array = ArrayBuffer[InputData]()
    for (a <- 0 until length){
      var obj = new InputData()
      obj.id = a+1
      obj.value = result(a)
      array += obj
    }
    selSort(array)

    val path = "./src/main/resources/"
    val writer = new PrintWriter(new File(path + "selectionSortOutput.csv" ))
    
    writer.println("id|value")
    for(l <- array)
    {
      writer.println(l.id + "|" + l.value)
    }

    writer.close()
    var dfDone = sparkSession.read.option("delimiter","|").option("header", "true").csv("./src/main/resources/selectionSortOutput.csv")
    
    val t_stop = System.currentTimeMillis()
    println("Selection sort time: " + (t_stop - t_start) + " ms")
    dfDone.show(10, false)
  }

  def sparkSort: Unit = {
    val t_start = System.currentTimeMillis()
    var splittedPairs = sparkSession.sql("SELECT cast(data[0] as integer) as id, cast(data[1] as float) as value FROM pairs_view ORDER BY value")

    val path = "./src/main/resources/"

    val t_stop = System.currentTimeMillis()
    println("Pure spark sort time: " + (t_stop - t_start) + " ms")
  }

  def quickSortMaint: Unit = {
    val t_start = System.currentTimeMillis()
    var splittedPairs = sparkSession.sql("SELECT cast(data[0] as integer) as id, cast(data[1] as float) as value FROM pairs_view")
    splittedPairs.createOrReplaceTempView("source_table")
    val len = splittedPairs.count().toInt
    var result = splittedPairs.toDF().select("value").rdd.map(r => r(0).asInstanceOf[Float]).collect()
    var array = ArrayBuffer[InputData]()
    for (a <- 0 until len){
      var obj = new InputData()
      obj.id = a+1
      obj.value = result(a)
      array += obj
    }
    // quickSort(array)
    array = QuickSortNet(array)


    val path = "./src/main/resources/"
    val writer = new PrintWriter(new File(path + "quickSortOutput.csv" ))

    writer.println("id|value")
    for(l <- array)
    {
      writer.println(l.id + "|" + l.value)
    }
    writer.close()
    var dfDone = sparkSession.read.option("delimiter","|").option("header", "true").csv("./src/main/resources/quickSortOutput.csv")
    val t_stop = System.currentTimeMillis()
    println("Quick sort execution time: " + (t_stop - t_start) + " ms")
    dfDone.show(10, false)
   }

  class InputData{
    var id = 0
    var value = 0.0
  }

  def QuickSortNet(a:ArrayBuffer[InputData]): ArrayBuffer[InputData] =
    if (a.length < 2) a
    else {
        var smaller = ArrayBuffer[InputData]()
        var bigger = ArrayBuffer[InputData]()
        var pivotList = ArrayBuffer[InputData]()
        var pivot = a(0).value
        for(i <-0 to a.length-1){
          if (a(i).value < pivot){
            smaller += a(i)
          }
          else if (a(i).value > pivot){
            bigger += a(i)
          }
          else {
            pivotList += a(i)
          }
        }
        smaller = QuickSortNet(smaller)
        bigger = QuickSortNet(bigger)
        smaller++pivotList++bigger
    }
  
  // def selSort(a:ArrayBuffer[InputData]): ArrayBuffer[InputData] =
  //   if (a.length < 2) a //jesli jeden element lub puste to zwroc
  //   else {
  //     var SortedSelection = a.clone()
  //     for (i <- 0 to (a.length - 1)) {
  //       var minvalue  = 10000000.0
  //       var minId = a(0)
  //       for (j <- 0 to (a.drop(i).length - 1)){
  //         if(a(j).value < minvalue){
  //           minvalue = a(j).value
  //           minId = a(j+i)
  //         }
  //       }
  //       // var minvalue  = SortedSelection.drop(i).min

  //       var minIndex = SortedSelection.indexOf(minId)
  //       //println(minIndex)
  //       var placeholder = SortedSelection(minIndex)
  //       SortedSelection(minIndex) = SortedSelection(i)
  //       SortedSelection(i) = placeholder
  //     }
  //     // println(5)
  //     SortedSelection
  //   }

}