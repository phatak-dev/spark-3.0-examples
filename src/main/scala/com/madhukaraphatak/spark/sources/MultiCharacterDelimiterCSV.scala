package com.madhukaraphatak.spark.sources

import org.apache.spark.sql.SparkSession

object MultiCharacterDelimiterCSV {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
          master("local")
          .appName("csvexample")
          .getOrCreate()

    val df  = sparkSession.read
      .option("delimiter","||")
      .option("header","true")
      .csv("src/main/resources/multicharacterseperator.csv")

    df.show()

  }

}
