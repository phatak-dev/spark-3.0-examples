package com.madhukaraphatak.spark.core.plugins.dynamicconfig

object Configuration {

  private var value = 10

  def getConfig: Int = value

  def changeConfig(newValue : Int):Int = {value = newValue; value}

}
