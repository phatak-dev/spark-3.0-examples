package com.madhukaraphatak.spark.core.plugins.driveronly

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DriverPluginExample {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .set("spark.plugins","com.madhukaraphatak.spark.core.plugins.driveronly.CustomSparkPlugin")
      .setAppName("executor plugin example")


    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    val df = sparkSession.range(5000)

    //cache the table
    df.createOrReplaceTempView("test")
    sparkSession.catalog.cacheTable("test")

    df.count()

    Thread.sleep(10000)

    sparkSession.stop()
  }
}
