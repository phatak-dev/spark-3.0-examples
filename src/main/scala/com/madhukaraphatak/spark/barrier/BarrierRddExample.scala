package com.madhukaraphatak.spark.barrier

import org.apache.spark.sql.SparkSession

object BarrierRddExample {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
          master("local[4]")
          .appName("example")
          .getOrCreate()

    val df = sparkSession.range(0,100).repartition(4)

    val barrierRdd = df.rdd.barrier()

    val count = barrierRdd.mapPartitions(v => v).count()

    println("count is " + count)

  }

}
