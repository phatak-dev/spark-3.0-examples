package com.madhukaraphatak.spark.ml

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.DataFrame

object MLUtils {

  def accuracyScore(df: DataFrame, label: String, predictCol: String) = {
    val rdd = df.select(predictCol,label).rdd.map(row ⇒ (row.getDouble(0), row.getInt(1).toDouble))
    new MulticlassMetrics(rdd).accuracy
  }
  def recall(df: DataFrame, labelCol: String, predictCol: String, labelValue:Double) = {
    val rdd = df.select(predictCol,labelCol).rdd.map(row ⇒ (row.getDouble(0), row.getInt(1).toDouble))
    new MulticlassMetrics(rdd).recall(labelValue)
  }

  def trainTestSplit(df:DataFrame, testSize:Double = 0.3):(DataFrame,DataFrame) = {
    val dfs = df.randomSplit(Array(1-testSize, testSize))
    val trainDf = dfs(0)
    val crossDf = dfs(1)
    (trainDf,crossDf)
  }

}

