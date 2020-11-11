package com.madhukaraphatak.spark.barrier

import org.apache.spark.sql.SparkSession

object BarrierExceptionExample {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
          master("local")
          .appName("example")
          .getOrCreate()

    val df = sparkSession.range(0,100).repartition(4)

    val barrierRdd = df.rdd.barrier()

    //fails running as it needs minimum four cores for four partitions
    val count = barrierRdd.mapPartitions(v => v).count()

    println("count is " + count)

  }

}
