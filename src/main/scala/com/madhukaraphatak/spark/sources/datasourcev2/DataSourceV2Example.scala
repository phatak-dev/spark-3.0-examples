package com.madhukaraphatak.spark.sources.datasourcev2

import org.apache.spark.Partition
import org.apache.spark.sql.{SaveMode, SparkSession}
import shapeless.Tuple

object DataSourceV2Example {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local[2]")
      .appName("example")
      .getOrCreate()

    val simpleDf = sparkSession.read
      .format("com.madhukaraphatak.spark.sources.datasourcev2.simple")
      .load()

    simpleDf.show()
    println(
      "number of partitions in simple source is " + simpleDf.rdd.getNumPartitions)


    val simpleMultiDf = sparkSession.read
      .format("com.madhukaraphatak.spark.sources.datasourcev2.simplemulti")
      .load()

    simpleMultiDf.show()
    println(
      "number of partitions in simple multi source is " + simpleMultiDf.rdd.getNumPartitions)


    val simpleCsvDf = sparkSession.read
      .format("com.madhukaraphatak.spark.sources.datasourcev2.simplecsv")
      .load("src/main/resources/adult.csv")

    simpleCsvDf.printSchema()
    simpleCsvDf.show()
    println(
      "number of partitions in simple csv source is " + simpleCsvDf.rdd.getNumPartitions)




    val simpleMysqlDf = sparkSession.createDataFrame(Seq(
      Tuple1("test1"),
      Tuple1("test2")
    )).toDF("user")

    //write examples
    simpleMysqlDf.write
      .format(
        "com.madhukaraphatak.spark.sources.datasourcev2.simplemysqlwriter")
      .mode(SaveMode.Append)
      .save()
/*
    simpleMysqlDf.write
      .format(
        "com.madhukaraphatak.examples.sparktwo.datasourcev2.mysqlwithtransaction")
      .save()

    val simplePartitoningDf = sparkSession.read
      .format(
        "com.madhukaraphatak.examples.sparktwo.datasourcev2.partitionaffinity")
      .load()

    val dfRDD = simplePartitoningDf.rdd
    val baseRDD =
      dfRDD.dependencies.head.rdd.dependencies.head.rdd.dependencies.head.rdd

    val partition = baseRDD.partitions(0)
    val getPrefferedLocationDef = baseRDD.getClass
      .getMethod("getPreferredLocations", classOf[Partition])
    val preferredLocation = getPrefferedLocationDef
      .invoke(baseRDD, partition)
      .asInstanceOf[Seq[String]]
    println("preferred location is " + preferredLocation)

    */

    sparkSession.stop()

  }
}
