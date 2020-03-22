package com.madhukaraphatak.spark.sources

import org.apache.spark.sql.SparkSession

object BinaryFile {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().
      appName("simple").master("local").getOrCreate()

    val df = sparkSession.read.format("binaryFile")
        .load("/home/madhu/Downloads/IMG_20190506_210110-EFFECTS.jpg")

    df.select("content").show()

  }

}
