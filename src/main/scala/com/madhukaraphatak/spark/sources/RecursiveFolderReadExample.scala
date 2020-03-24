package com.madhukaraphatak.spark.sources

import org.apache.spark.sql.SparkSession

object RecursiveFolderReadExample {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
          master("local")
          .appName("csvexample")
          .getOrCreate()


    // normal read

    val df  = sparkSession.read
      .option("delimiter","||")
      .option("header","true")
      .csv("src/main/resources/nested")

    assert(df.count() == 2)

    // recursive read
     val recursiveDf  = sparkSession.read
      .option("delimiter","||")
       .option("recursiveFileLookup","true")
      .option("header","true")
      .csv("src/main/resources/nested")

    assert(recursiveDf.count() == 4)

  }

}
