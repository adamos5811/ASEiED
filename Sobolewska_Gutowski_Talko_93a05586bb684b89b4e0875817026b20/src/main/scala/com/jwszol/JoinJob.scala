package com.jwszol


// $example on$

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}

import org.apache.spark.mllib.util.MLUtils.loadLibSVMFile
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
// $example off$



object NaiveBayesExample {

  def main(args: Array[String]): Unit = {

   


    val conf = new SparkConf().setMaster("local[2]") 
                    .setAppName("NaiveBayesExample")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc) //tworzymy context do operacji sql na danych
    // $example on$

    // Load and parse the data file.

    val data = loadLibSVMFile(sc, "./src/main/resources/sample_libsvm_data.txt")

    val path = "./src/main/resources/dataMay-31-2017.json" //wczytywanie json
    val MapPartRDD = sc.wholeTextFiles(path).values
    val rawData = sqlContext.read.json(MapPartRDD)
    val extractedData = rawData.withColumn("data", explode(rawData.col("data"))).select("data")
    extractedData.createOrReplaceTempView("pairs_view")

    val punkty1x = sqlContext.sql("SELECT cast(data[0] as float) as x FROM pairs_view").cache() //ladujemy punkty
    val punkty1y = sqlContext.sql("SELECT cast(data[1] as float) as y FROM pairs_view").cache()
    val punkty2x = sqlContext.sql("SELECT cast(data[2] as float) as x FROM pairs_view").cache()
    val punkty2y = sqlContext.sql("SELECT cast(data[3] as float) as y FROM pairs_view").cache()
    
    punkty1x.collect().foreach(println) //collect daje nam dostep do punktow

    // Split data into training (60%) and test (40%).

    val Array(training, test) = data.randomSplit(Array(0.6, 0.4))



    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")



    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))

    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()



    // Save and load model

    // model.save(sc, "./src/main/resources/myNaiveBayesModel")

    // val sameModel = NaiveBayesModel.load(sc, "./src/main/resources/myNaiveBayesModel")

    // $example off$



    sc.stop()

  }

}

