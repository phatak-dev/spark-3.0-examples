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


    // min by window function

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.dense_rank

    val orderedDf = Window.orderBy(df.col("value"))
    val rankedDf = df.withColumn("rank", dense_rank.over(orderedDf))
    val minDf = rankedDf.filter("rank == 1")
    minDf.show()



    // find the id which has maximum value

    val resultDf = sparkSession.sql("select max_by(id,value) max_id, min_by(id,value) min_id from table")

    resultDf.show()




  }

}
