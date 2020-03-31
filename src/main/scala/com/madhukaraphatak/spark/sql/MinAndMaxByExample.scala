package com.madhukaraphatak.spark.sql

import org.apache.spark.sql.SparkSession


object MinAndMaxByExample {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("min and max by example")
      .getOrCreate()


    val df = sparkSession.createDataFrame(Seq(
      ("1", 10),
      ("2", 20),
      ("3", 30),
      ("4", 40)
    )).toDF("id","value")
    df.createOrReplaceTempView("table")

    // find the id which has maximum value

    val resultDf = sparkSession.sql("select max_by(id,value) max_id, min_by(id,value) min_id from table")

    resultDf.show()




  }

}
