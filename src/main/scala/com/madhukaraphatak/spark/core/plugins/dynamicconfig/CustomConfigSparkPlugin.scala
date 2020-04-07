package com.madhukaraphatak.spark.core.plugins.dynamicconfig

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, SparkPlugin}

class CustomConfigSparkPlugin extends SparkPlugin{
  override def driverPlugin(): DriverPlugin = new CustomConfigDriverPlugin

  override def executorPlugin(): ExecutorPlugin = null
}
