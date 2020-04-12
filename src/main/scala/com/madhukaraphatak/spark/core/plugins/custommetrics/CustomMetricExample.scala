package com.madhukaraphatak.spark.core.plugins.custommetrics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CustomMetricExample {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .set("spark.plugins","com.madhukaraphatak.spark.core.plugins.custommetrics.CustomMetricSparkPlugin")
       .set("spark.metrics.conf","src/main/resources/metric.properties")
      .setAppName("executor plugin example")


    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    val df = sparkSession.range(5000).repartition(5)

    val incrementedDf = df.mapPartitions(iterator => {
      var evenCount = 0
      val incrementedIterator = iterator.toList.map(value => {
        if(value % 2 == 0) evenCount = evenCount +1
        value +1
      }).toIterator
      CustomMetricSparkPlugin.value.set(evenCount)
      incrementedIterator
    })


    incrementedDf.count()


  }
}
