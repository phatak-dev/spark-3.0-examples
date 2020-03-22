package com.madhukaraphatak.spark.core

import org.apache.spark.sql.SparkSession

object BarrierExample {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().
      appName("simple").master("local[4]").getOrCreate()

    val df = sparkSession.range(100).repartition(2)

    //run barrier mode

    val barrierRDD = df.rdd.barrier()

    barrierRDD.mapPartitionsWithIndex{
      case (index,value) => {
      //first wait for 10s
      Thread.sleep(10000)
      if(index == 1) throw new IllegalArgumentException
      Thread.sleep(100000)
      value
    }}.count()




  }

}
