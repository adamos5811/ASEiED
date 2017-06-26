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

  
  def selSort(xs: ArrayBuffer[DataType], len: Int){
    def swap(i: Int, j: Int){
        val t = xs(i); xs(i) = xs(j); xs(j) = t
        }

    def minimal(i: Int){
        for(j <- i until len){
            if(xs(j).value < xs(i).value) swap(i, j)
            }
        }
    
    def sorting(){
        for(i <- 0 until len){
            minimal(i)
            }
        }
    sorting()
    }

  def selectionSort: Unit = {
    
    println("Selection sort start")
    val t0 = System.currentTimeMillis()

    val splittedPairs = sparkSession.sql("SELECT cast(data[0] as integer) as id, cast(data[1] as float) as value FROM pairs_view")
    splittedPairs.createOrReplaceTempView("source_table")
    val length = splittedPairs.count().toInt
    
    var result = splittedPairs.toDF().select("value").rdd.map(r => r(0).asInstanceOf[Float]).collect()
    var array = ArrayBuffer[DataType]()
    for (a <- 0 until length){
      var obj = new DataType()
      obj.id = a+1
      obj.value = result(a)
      array += obj
    }
    selSort(array, length)

    val path = "./src/main/resources/"
    val writer = new PrintWriter(new File(path + "selectionSortOutput.csv" ))
    
    writer.println("id|value")
    for(l <- array)
    {
      writer.println(l.id + "|" + l.value)
    }

    writer.close()
    var dfDone = sparkSession.read.option("delimiter","|").option("header", "true").csv("./src/main/resources/selectionSortOutput.csv")
    
    val t1 = System.currentTimeMillis()
    println("Execution time of selection sort: " + (t1 - t0) + " ms")
    dfDone.show()
   val outputFile = dfDone.toJSON.collect()
    val writer2 = new PrintWriter(new File(path + "selectionSortOutput.json" ))

    for(l <- outputFile)
    {
      writer2.println(l)
    }

    writer2.close()
  }

  def sparkSort: Unit = {
    println("Spark SQL sort clock start")
    val t0 = System.currentTimeMillis()
    var splittedPairs = sparkSession.sql("SELECT cast(data[0] as integer) as id, cast(data[1] as float) as value FROM pairs_view ORDER BY value")

    val outputFile = splittedPairs.toJSON.collect()
    val path = "./src/main/resources/"
    val writer = new PrintWriter(new File(path + "sparkPureSortOutput.json" ))

    for(l <- outputFile)
    {
      writer.println(l)
    }

    writer.close()

    val t1 = System.currentTimeMillis()
    println("Spark SQL sort execution time: " + (t1 - t0) + " ms")
  }

  def quickSortMaint: Unit = {
    val t0 = System.currentTimeMillis()
    println("Quick sort clock start")
    var splittedPairs = sparkSession.sql("SELECT cast(data[0] as integer) as id, cast(data[1] as float) as value FROM pairs_view")
    splittedPairs.createOrReplaceTempView("source_table")
    val len = splittedPairs.count().toInt
    var result = splittedPairs.toDF().select("value").rdd.map(r => r(0).asInstanceOf[Float]).collect()
    var array = ArrayBuffer[DataType]()
    for (a <- 0 until len){
      var obj = new DataType()
      obj.id = a+1
      obj.value = result(a)
      array += obj
    }
    quickSort(array)

    val path = "./src/main/resources/"
    val writer = new PrintWriter(new File(path + "quickSortOutput.csv" ))

    writer.println("id|value")
    for(l <- array)
    {
      writer.println(l.id + "|" + l.value)
    }
    writer.close()
    var dfDone = sparkSession.read.option("delimiter","|").option("header", "true").csv("./src/main/resources/quickSortOutput.csv")
    val t1 = System.currentTimeMillis()
    println("Quick sort execution time: " + (t1 - t0) + " ms")
    dfDone.show()
    val outputFile = dfDone.toJSON.collect()
    val writer2 = new PrintWriter(new File(path + "quickSortOutput.json" ))

    for(l <- outputFile)
    {
      writer2.println(l)
    }

    writer2.close()
  }

  class DataType{
    var id = 0
    var value = 0.0
  }


  def quickSort(xs: ArrayBuffer[DataType]) {

    def swap(i: Int, j: Int) {
      val t = xs(i); xs(i) = xs(j); xs(j) = t
    }

    def sorting(l: Int, r: Int) {
      val pivot = xs((l + r) / 2).value
      var i = l; var j = r
      while (i <= j) {
        while (xs(i).value < pivot) i += 1
        while (xs(j).value > pivot) j -= 1
        if (i <= j) {
          swap(i, j)
          i += 1
          j -= 1
        }
      }
      if (l < j) sorting(l, j)
      if (j < r) sorting(i, r)
    }

    sorting(0, xs.length - 1)
  }
}