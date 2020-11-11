package com.madhukaraphatak.spark.barrier

import org.apache.spark.BarrierTaskContext
import org.apache.spark.sql.SparkSession

object BarrierMethodExample {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
          master("local[4]")
          .appName("example")
          .getOrCreate()

    val df = sparkSession.range(0,100).repartition(4)

    val barrierRdd = df.rdd.barrier()

    val mappedRDD = barrierRdd.mapPartitionsWithIndex{ case (index,iterator) => {
      val taskContext = BarrierTaskContext.get()  
      taskContext.barrier()
      println("barrier context completed")
      iterator
    }}

    mappedRDD.count()

  }

}
