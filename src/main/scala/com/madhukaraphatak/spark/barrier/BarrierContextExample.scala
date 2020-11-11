package com.madhukaraphatak.spark.barrier

import org.apache.spark.{BarrierTaskContext, TaskContext}
import org.apache.spark.sql.SparkSession

object BarrierContextExample {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
          master("local[4]")
          .appName("example")
          .getOrCreate()

    val df = sparkSession.range(0,100).repartition(4)

    val barrierRdd = df.rdd.barrier()

    val mappedRDD = barrierRdd.mapPartitionsWithIndex{ case (index,iterator) => {
      val taskContext = BarrierTaskContext.get()
      val taskInfos = taskContext.getTaskInfos().map(_.address)
      println(taskInfos)
      iterator
    }}

    mappedRDD.count()

  }

}
