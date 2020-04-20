package com.madhukaraphatak.spark.sql

import org.apache.spark.sql.SparkSession

object JoinHintsExample {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
          master("local")
          .appName("join hits example")
          .getOrCreate()

    val salesDf = sparkSession.read.
      format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/sales.csv")


    val customerDf = sparkSession.read.
      format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/customers.csv")


    //broadcast hint

    val broadcastJoin = salesDf.hint("broadcast").join(customerDf,"customerId")
    broadcastJoin.show()

    // merge join

    val mergeJoin = salesDf.hint("merge").join(customerDf, "customerId")
    mergeJoin.show()

    // shuffle_hash

    val shuffleHashJoin = salesDf.hint("shuffle_hash").join(customerDf,"customerId")
    shuffleHashJoin.show()

    //shuffle_replicate_nl
    val cartesianProduct = salesDf.hint("shuffle_replicate_nl").join(customerDf)
    cartesianProduct.show()


    //Thread.sleep(1000000)


  }

}
