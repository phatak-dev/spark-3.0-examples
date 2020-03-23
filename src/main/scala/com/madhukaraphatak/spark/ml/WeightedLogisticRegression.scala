package com.madhukaraphatak.spark.ml

import org.apache.spark.ml._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, SparkSession}
import MLUtils._
import org.apache.spark.sql.functions._

/**
 * Weighted Logistic Regression for Credit Card Fraud
 *
 */
object WeightedLogisticRegression {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local[4]")
      .appName("example")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    //load train df
    // Download the data from : https://www.kaggle.com/dalpozz/creditcardfraud/downloads/creditcard.csv
    val df = sparkSession.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/creditcard.csv")
    df.printSchema()

    val amountVectorAssembler = new VectorAssembler().setInputCols(Array("Amount")).setOutputCol("Amount_vector")
    val standarScaler = new StandardScaler().setInputCol("Amount_vector").setOutputCol("Amount_scaled")
    val dropColumns = Array("Time","Amount","Class")
    
    val cols = df.columns.filter( column => !dropColumns.contains(column)) ++ Array("Amount_scaled")
    val vectorAssembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")

    // pipeline 
    val logisticRegression = new LogisticRegression().setLabelCol("Class")
    val trainPipeline = new Pipeline().setStages(Array(amountVectorAssembler,standarScaler,vectorAssembler,logisticRegression))

    println("for imbalanced data")
    runPipeline(trainPipeline, df)

    // add weight column
    val ratioOfFraud = getRatio(df)
    val fraudWeight  = 1 - ratioOfFraud
    val nonFraudWeight = ratioOfFraud

    val weightedDF = df.withColumn("weight",
      when(df.col("Class").===("1.0"),fraudWeight)
     .otherwise(nonFraudWeight))

    logisticRegression.setWeightCol("weight")
    println("for balanced data")
    val balancedModel = runPipeline(trainPipeline, weightedDF)

    println("balanced model for full data")
    printScores(balancedModel, weightedDF)

   }


  def getRatio(df:DataFrame) = {
    val fraudDf = df.filter("Class=1.0")
    val nonFraudDf = df.filter("Class=0.0")
    //random sample the nonFraud to match the value of fraud
    val sampleRatio = fraudDf.count().toDouble / df.count().toDouble
    sampleRatio
  }

  def runPipeline(pipeline:Pipeline, df:DataFrame):PipelineModel = {
    val (trainDf,crossDf) = trainTestSplit(df)
    val model = pipeline.fit(trainDf)
    printScores(model, crossDf)
    model
  }

  def printScores(model:PipelineModel, df:DataFrame) = {
    println("test accuracy with pipeline " + accuracyScore(model.transform(df), "Class", "prediction"))
    println("test recall for 1.0 is " + recall(model.transform(df), "Class", "prediction", 1.0))
  }
}
 
