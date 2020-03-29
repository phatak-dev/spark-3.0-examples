package com.madhukaraphatak.spark.sql

import org.apache.spark.sql.SparkSession

object InMemoryTableScanExample {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
      master("local[2]")
      .appName("in memory table in UI example")
      .getOrCreate()


    val firstDF = sparkSession.createDataFrame(Seq(
      ("1", 10),
      ("2", 20)
    )).toDF("id", "sales")

    firstDF.createOrReplaceTempView("firstDf")

    sparkSession.catalog.cacheTable("firstDf")

    val secondDF = sparkSession.createDataFrame(Seq(
      ("1", 40),
      ("2", 50)
    )).toDF("id", "volume")

    secondDF.createOrReplaceTempView("secondDf")
    sparkSession.catalog.cacheTable("secondDf")

    val joinDF = firstDF.join(secondDF, "id")

    joinDF.count()

  }

}
