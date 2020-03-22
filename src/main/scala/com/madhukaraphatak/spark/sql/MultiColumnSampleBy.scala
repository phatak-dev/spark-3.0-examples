package com.madhukaraphatak.spark.sql

import org.apache.spark.sql.{Row, SparkSession}

object MultiColumnSampleBy {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("multi column sample")
      .getOrCreate()

    val df = sparkSession.createDataFrame(Seq(
      (1,"p1", "s1",20),
      (1,"p2", "s1",30),
      (1,"p1", "s2",40),
      (1,"p2","s2",50),
      (2,"p1", "s1",20),
      (2,"p2", "s1",30),
      (2,"p1", "s2",40),
      (2,"p2","s2",50)))
      .toDF("day","product", "store","sales")

    //single sampleBy

    val singleFractions = Map("p1" -> 0.5, "p2" -> 0.5)
    val singleSampleDf = df.stat.sampleBy("product",singleFractions,-1)
    singleSampleDf.sort("product").show()

    // multi column sampleBy on product and store
    import org.apache.spark.sql.functions.struct
    val multipleFractions = Map(Row("p1","s1") -> 0.5,
      Row("p1","s1")->0.5,
      Row("p1","s2") -> 0.5,
      Row("p2","s2") -> 0.5)
    val multiSampleDf = df.stat.sampleBy(struct("product","store"),multipleFractions,-1)
    multiSampleDf.show()


  }

}
