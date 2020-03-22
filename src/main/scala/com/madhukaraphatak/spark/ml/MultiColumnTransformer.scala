package com.madhukaraphatak.spark.ml

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession

object MultiColumnTransformer {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()


    val salaryDf = sparkSession.read.format("csv").option("header", "true").load("src/main/resources/adult.csv")

    val inputColumns = Array("workclass","education")

    val outputColumns = Array("workclass_indexed", "education_indexed")


    // indexer multiple column
    val stringIndexer = new StringIndexer()
    stringIndexer.setInputCols(inputColumns)
    stringIndexer.setOutputCols(outputColumns)
    val indexedDf = stringIndexer.fit(salaryDf).transform(salaryDf)

    indexedDf.select(outputColumns.head, outputColumns.tail:_*).show()

  }

}
