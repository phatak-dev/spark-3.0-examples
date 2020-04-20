package com.madhukaraphatak.spark.sql

import org.apache.spark.sql.SparkSession

object DataFrameTail {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().
      appName("example").master("local").getOrCreate()

    val df = sparkSession.range(100)

    //head
    println(df.head(2).toList)

    println(df.tail(5).toList)


  }

}
