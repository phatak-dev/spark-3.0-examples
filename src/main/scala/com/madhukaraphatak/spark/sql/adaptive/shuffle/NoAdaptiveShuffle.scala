package com.madhukaraphatak.spark.sql.adaptive.shuffle

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object NoAdaptiveShuffle {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("test plan")
      .setMaster("local[2]")
    //.set("spark.sql.adaptive.enabled","true")
    //.set("spark.sql.runtime.reoptimization.enabled","true")

    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df = sparkSession.read.
      format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/sales.csv").repartition(500)


    df.groupBy("customerId").count().count()

    //val joinDf  = firstDf.join(secondDf,firstDf.col("customerId") === secondDf.col("customerId"),"left")

    // println("number of partitions "+joinDf.rdd.partitions.length)

    //joinDf.count()

    Thread.sleep(1000000)


  }

}
