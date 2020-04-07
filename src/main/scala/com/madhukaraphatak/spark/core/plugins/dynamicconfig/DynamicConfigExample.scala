package com.madhukaraphatak.spark.core.plugins.dynamicconfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object DynamicConfigExample {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .set("spark.plugins","com.madhukaraphatak.spark.core.plugins.dynamicconfig.CustomConfigSparkPlugin")
      .setAppName("executor plugin example")



    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    import sparkSession.implicits._

    val df = sparkSession.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",8888).load().as[String]

    val returnDf = df.map(value => value + Configuration.getConfig)

    val query = returnDf.writeStream.
      queryName("something")
      .format("console")
      .outputMode(OutputMode.Append())

    query.start().awaitTermination()

  }
}
