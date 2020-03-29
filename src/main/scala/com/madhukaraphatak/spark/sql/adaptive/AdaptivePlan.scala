package com.madhukaraphatak.spark.sql.adaptive

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AdaptivePlan {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("test plan")
      .setMaster("local[2]")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.runtime.reoptimization.enabled", "true")

    val sparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df = sparkSession.range(0, 100)

    df.explain(true)


  }

}
