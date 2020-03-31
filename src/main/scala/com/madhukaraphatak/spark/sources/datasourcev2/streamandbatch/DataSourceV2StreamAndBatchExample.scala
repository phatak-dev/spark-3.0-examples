package com.madhukaraphatak.spark.sources.datasourcev2.streamandbatch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object DataSourceV2StreamAndBatchExample {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.
          master("local[2]")
          .appName("streaming example")
          .getOrCreate()


    val dataSource = "com.madhukaraphatak.spark.sources.datasourcev2.streamandbatch.simple"

    val batchDf = sparkSession
      .read
      .format(dataSource)
      .load()

    batchDf.show()

    val streamingDf = sparkSession.
      readStream.
      format(dataSource)
      .load()

    val query = streamingDf.writeStream
      .format("console")
      .queryName("simple_source")
    .outputMode(OutputMode.Append())

    query.start().awaitTermination()

  }

}
