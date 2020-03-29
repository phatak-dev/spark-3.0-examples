package com.madhukaraphatak.spark.core.plugins.custommetrics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CustomMetricExample {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .set("spark.plugins","com.madhukaraphatak.spark.core.plugins.custommetrics.CustomMetricSparkPlugin")
      .set("spark.metrics.conf","src/main/resources/metric.properties")
      .setAppName("executor plugin example")


    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    val df = sparkSession.range(5000).repartition(5)

    df.count()


    sparkSession.stop()

  }
}
